"""
ingestao.py - Coleta dados brutos para o projeto MedData.
"""

import argparse
import sys
from pathlib import Path

import oci
import pandas as pd
from oci.config import from_file
from pysus import cnes, sih

# Permite importar config.py da pasta src
sys.path.append(str(Path(__file__).parent))

from config import config


def upload_para_object_storage(arquivo_local, objeto_name, bucket="meddata-bronze"):
    """Envia um arquivo para o Object Storage da OCI."""
    try:
        config_oci = from_file()
        object_storage = oci.object_storage.ObjectStorageClient(config_oci)
        namespace = object_storage.get_namespace().data

        with open(arquivo_local, "rb") as arquivo:
            object_storage.put_object(
                namespace,
                bucket,
                objeto_name,
                arquivo,
            )

        print(f"[OCI] Upload concluído: {bucket}/{objeto_name}")
        return True

    except Exception as erro:
        print(f"[ERRO OCI] Falha no upload: {erro}")
        return False


def baixar_sih(uf=None, ano=None, mes=None, upload=True):
    """Baixa dados de internações do SIH/SUS."""
    uf = uf or config.UF
    ano = ano or config.ANO
    mes = mes or config.MES

    print(f"[SIH] Baixando dados: {uf} {ano}/{mes:02d}")

    try:
        arquivos = sih(state=uf, year=ano, month=mes)

        if not arquivos:
            print(f"[SIH] Nenhum arquivo encontrado para {uf} {ano}/{mes:02d}")
            return None

        df = pd.read_parquet(arquivos[0])
        print(f"[SIH] Carregado: {len(df):,} registros")

        caminho_raw = config.RAW_DIR / config.get_nome_arquivo(
            "sih",
            uf=uf,
            ano=ano,
            mes=mes,
        )

        df.to_parquet(caminho_raw, index=False)
        print(f"[SIH] Salvo localmente em: {caminho_raw}")

        if upload:
            objeto_name = f"sih/{uf}/{ano}/sih_{uf}_{ano}_{mes:02d}.parquet"
            upload_para_object_storage(caminho_raw, objeto_name)

        return df

    except Exception as erro:
        print(f"[ERRO SIH] {erro}")
        return None


def baixar_cnes_leitos(uf=None, ano=None, mes=None, upload=True):
    """Baixa dados de leitos hospitalares do CNES."""
    uf = uf or config.UF
    ano = ano or config.ANO
    mes = mes or config.MES

    print(f"[CNES] Baixando dados: {uf} {ano}/{mes:02d}")

    try:
        arquivos = cnes(
            state=uf,
            year=ano,
            month=mes,
            group="LT",
        )

        if not arquivos:
            print(f"[CNES] Nenhum arquivo encontrado para {uf} {ano}/{mes:02d}")
            return None

        df = pd.read_parquet(arquivos[0])
        print(f"[CNES] Carregado: {len(df):,} registros")

        caminho_raw = config.RAW_DIR / config.get_nome_arquivo(
            "cnes",
            uf=uf,
            ano=ano,
            mes=mes,
        )

        df.to_parquet(caminho_raw, index=False)
        print(f"[CNES] Salvo localmente em: {caminho_raw}")

        if upload:
            objeto_name = f"cnes/{uf}/{ano}/cnes_{uf}_{ano}_{mes:02d}.parquet"
            upload_para_object_storage(caminho_raw, objeto_name)

        return df

    except Exception as erro:
        print(f"[ERRO CNES] {erro}")
        return None


def baixar_ibge(uf=None, upload=True):
    """Baixa a referência de municípios do IBGE."""
    uf = (uf or config.UF).upper()

    print(f"[IBGE] Carregando dados para {uf}")

    try:
        codigo_uf = config.get_codigo_uf(uf)

        if codigo_uf is None:
            raise ValueError(f"UF '{uf}' não encontrada.")

        url = (
            "https://raw.githubusercontent.com/"
            "kelvins/municipios-brasileiros/main/csv/municipios.csv"
        )

        df = pd.read_csv(url)
        df = df[df["codigo_uf"] == codigo_uf].copy()

        if df.empty:
            raise ValueError(f"Nenhum município encontrado para UF '{uf}'.")

        codigo_para_uf = {
            11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA",
            16: "AP", 17: "TO", 21: "MA", 22: "PI", 23: "CE",
            24: "RN", 25: "PB", 26: "PE", 27: "AL", 28: "SE",
            29: "BA", 31: "MG", 32: "ES", 33: "RJ", 35: "SP",
            41: "PR", 42: "SC", 43: "RS", 50: "MS", 51: "MT",
            52: "GO", 53: "DF",
        }

        df["uf"] = df["codigo_uf"].map(codigo_para_uf).fillna("NA")
        print(f"[IBGE] Carregado: {len(df):,} municípios")

        caminho_referencia = config.REFERENCE_DIR / f"ibge_{uf}.parquet"
        df.to_parquet(caminho_referencia, index=False)
        print(f"[IBGE] Salvo localmente em: {caminho_referencia}")

        if upload:
            objeto_name = f"ibge/{uf}/ibge_{uf}.parquet"
            upload_para_object_storage(caminho_referencia, objeto_name)

        return df

    except Exception as erro:
        print(f"[ERRO IBGE] {erro}")
        return None


def baixar_todos(uf=None, ano=None, mes=None, upload=True):
    """Executa a ingestão das fontes SIH, CNES e IBGE."""
    uf = uf or config.UF
    ano = ano or config.ANO
    mes = mes or config.MES

    print("=" * 50)
    print("INICIANDO INGESTÃO DE DADOS")
    print(f"UF: {uf} | Ano: {ano} | Mês: {mes:02d}")
    print(f"Upload para OCI: {'SIM' if upload else 'NÃO'}")
    print("=" * 50)

    resultados = {
        "sih": baixar_sih(uf, ano, mes, upload),
        "cnes": baixar_cnes_leitos(uf, ano, mes, upload),
        "ibge": baixar_ibge(uf, upload),
    }

    print("=" * 50)
    print("RESUMO DA INGESTÃO")

    for nome, df in resultados.items():
        status = "OK" if df is not None else "FALHA"
        registros = len(df) if df is not None else 0
        print(f"{nome.upper():10} | {status:5} | {registros:>8,} registros")

    print("=" * 50)

    return resultados


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestão de dados do MedData")
    parser.add_argument("--uf", type=str, default=config.UF)
    parser.add_argument("--ano", type=int, default=config.ANO)
    parser.add_argument("--mes", type=int, default=config.MES)
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args()

    dados = baixar_todos(
        uf=args.uf,
        ano=args.ano,
        mes=args.mes,
        upload=args.upload,
    )

    todos_ok = all(df is not None for df in dados.values())
    sys.exit(0 if todos_ok else 1)