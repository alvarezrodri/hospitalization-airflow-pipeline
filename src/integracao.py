"""
integracao.py - Geração do Star Schema para o MedData.
"""

import logging
import sys
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).parent))

from config import config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def upload_para_object_storage(arquivo_local, objeto_name, bucket="meddata-gold"):
    """Envia um arquivo Gold ao Object Storage da OCI."""
    try:
        import oci
        from oci.config import from_file

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

        logger.info("Upload concluído: %s/%s", bucket, objeto_name)
        return True

    except Exception as erro:
        logger.error("Falha no upload para OCI: %s", erro)
        return False


def gerar_dim_municipio(df_ibge, df_sih):
    """
    Gera a dimensão de municípios a partir do IBGE-SP e inclui os
    municípios de residência presentes nas internações.
    """
    logger.info("Gerando DIM_MUNICIPIO...")

    dim = df_ibge.copy()
    dim = dim.drop_duplicates(subset=["codigo_municipio"])

    dim["nome_municipio"] = dim["nome_municipio"].fillna("Nao informado")
    dim["uf"] = dim["uf"].fillna("NA")
    dim["estado"] = dim["estado"].fillna("Nao informado")
    dim["latitude"] = dim["latitude"].fillna(0)
    dim["longitude"] = dim["longitude"].fillna(0)

    codigos_paciente = (
        df_sih["codigo_municipio_paciente"]
        .dropna()
        .drop_duplicates()
    )

    codigos_ausentes = codigos_paciente[
        ~codigos_paciente.isin(dim["codigo_municipio"])
    ]

    if not codigos_ausentes.empty:
        municipios_extras = pd.DataFrame(
            {
                "codigo_municipio": codigos_ausentes,
                "nome_municipio": "Nao informado",
                "uf": "NA",
                "estado": "Nao informado",
                "latitude": 0,
                "longitude": 0,
            }
        )

        dim = pd.concat([dim, municipios_extras], ignore_index=True)

        logger.info(
            "Incluídos %s municípios de residência sem cadastro IBGE-SP.",
            f"{len(municipios_extras):,}",
        )

    colunas = [
        "codigo_municipio",
        "nome_municipio",
        "uf",
        "estado",
        "latitude",
        "longitude",
    ]

    dim = dim[colunas].drop_duplicates(subset=["codigo_municipio"])

    logger.info("DIM_MUNICIPIO gerada: %s municípios", f"{len(dim):,}")
    return dim


def gerar_dim_hospital(df_cnes, df_municipios):
    """Gera a dimensão de hospitais a partir do CNES."""
    logger.info("Gerando DIM_HOSPITAL...")

    dim = df_cnes.copy()

    dim = dim.merge(
        df_municipios[
            [
                "codigo_municipio",
                "nome_municipio",
                "uf",
                "estado",
                "latitude",
                "longitude",
            ]
        ],
        on="codigo_municipio",
        how="left",
    ).rename(
        columns={
            "nome_municipio": "nome_municipio_hospital",
            "uf": "uf_hospital",
            "estado": "estado_hospital",
            "latitude": "latitude_hospital",
            "longitude": "longitude_hospital",
        }
    )

    dim["nome_municipio_hospital"] = (
        dim["nome_municipio_hospital"].fillna("Nao informado")
    )
    dim["uf_hospital"] = dim["uf_hospital"].fillna("NA")
    dim["estado_hospital"] = dim["estado_hospital"].fillna("Nao informado")
    dim["latitude_hospital"] = dim["latitude_hospital"].fillna(0)
    dim["longitude_hospital"] = dim["longitude_hospital"].fillna(0)

    dim = dim.drop_duplicates(subset=["id_hospital"])

    colunas_ordem = [
        "id_hospital",
        "codigo_municipio",
        "nome_municipio_hospital",
        "uf_hospital",
        "estado_hospital",
        "latitude_hospital",
        "longitude_hospital",
        "leitos_totais",
        "leitos_sus",
        "leitos_nao_sus",
        "esfera",
        "tipo_unidade",
        "nivel_hierarquico",
        "natureza_juridica",
        "porte_hospitalar",
        "alta_complexidade",
        "percentual_sus",
    ]

    colunas_existentes = [
        coluna for coluna in colunas_ordem if coluna in dim.columns
    ]

    dim = dim[colunas_existentes]

    logger.info("DIM_HOSPITAL gerada: %s hospitais", f"{len(dim):,}")
    return dim


def gerar_dim_tempo(df_sih):
    """Gera a dimensão de tempo a partir das datas de internação."""
    logger.info("Gerando DIM_TEMPO...")

    if "data_internacao" not in df_sih.columns:
        raise ValueError("A coluna 'data_internacao' não foi encontrada no SIH.")

    datas_unicas = pd.Series(
        pd.to_datetime(
            df_sih["data_internacao"],
            errors="coerce",
        ).dropna().unique()
    )

    if datas_unicas.empty:
        raise ValueError("Nenhuma data válida foi encontrada para gerar DIM_TEMPO.")

    dim = pd.DataFrame({"data_referencia": datas_unicas})
    dim = dim.sort_values("data_referencia").reset_index(drop=True)

    dim["ano"] = dim["data_referencia"].dt.year
    dim["mes"] = dim["data_referencia"].dt.month
    dim["trimestre"] = dim["data_referencia"].dt.quarter
    dim["dia_semana"] = dim["data_referencia"].dt.day_name()
    dim["ano_mes"] = dim["data_referencia"].dt.strftime("%Y-%m")
    dim["tempo_id"] = range(1, len(dim) + 1)

    logger.info("DIM_TEMPO gerada: %s datas únicas", f"{len(dim):,}")
    return dim


def calcular_distancia_km(lat1, lon1, lat2, lon2):
    """Calcula a distância aproximada entre dois pontos geográficos."""
    if any(pd.isna(valor) for valor in [lat1, lon1, lat2, lon2]):
        return np.nan

    raio_terra = 6371

    lat1, lon1, lat2, lon2 = map(
        radians,
        [lat1, lon1, lat2, lon2],
    )

    diferenca_latitude = lat2 - lat1
    diferenca_longitude = lon2 - lon1

    valor = (
        sin(diferenca_latitude / 2) ** 2
        + cos(lat1) * cos(lat2) * sin(diferenca_longitude / 2) ** 2
    )

    return raio_terra * 2 * atan2(sqrt(valor), sqrt(1 - valor))


def gerar_fato_internacao(df_sih, df_hospitais, df_municipios, df_tempo):
    """Gera a tabela FATO_INTERNACAO."""
    logger.info("Gerando FATO_INTERNACAO...")

    fato = df_sih.copy()

    fato = fato.merge(
        df_hospitais[
            [
                "id_hospital",
                "codigo_municipio",
                "nome_municipio_hospital",
                "latitude_hospital",
                "longitude_hospital",
                "leitos_sus",
            ]
        ],
        on="id_hospital",
        how="left",
    )

    fato = fato.merge(
        df_municipios[
            [
                "codigo_municipio",
                "nome_municipio",
                "uf",
                "estado",
                "latitude",
                "longitude",
            ]
        ],
        left_on="codigo_municipio_paciente",
        right_on="codigo_municipio",
        how="left",
        suffixes=("", "_paciente_ibge"),
    ).rename(
        columns={
            "nome_municipio": "nome_municipio_paciente",
            "uf": "uf_paciente",
            "estado": "estado_paciente",
            "latitude": "latitude_paciente",
            "longitude": "longitude_paciente",
        }
    )

    fato = fato.drop(
        columns=["codigo_municipio_paciente_ibge"],
        errors="ignore",
    )

    fato = fato.merge(
        df_tempo[["data_referencia", "tempo_id"]],
        left_on="data_internacao",
        right_on="data_referencia",
        how="inner",
    )

    fato = fato.drop(columns=["data_referencia"], errors="ignore")

    fato["distancia_estimada_km"] = fato.apply(
        lambda linha: calcular_distancia_km(
            linha.get("latitude_hospital", np.nan),
            linha.get("longitude_hospital", np.nan),
            linha.get("latitude_paciente", np.nan),
            linha.get("longitude_paciente", np.nan),
        ),
        axis=1,
    )

    campos_texto = {
        "nome_municipio_paciente": "Nao informado",
        "uf_paciente": "NA",
        "estado_paciente": "Nao informado",
        "nome_municipio_hospital": "Nao informado",
        "codigo_diagnostico": "Nao informado",
    }

    for coluna, valor_padrao in campos_texto.items():
        if coluna in fato.columns:
            fato[coluna] = fato[coluna].fillna(valor_padrao)

    campos_numericos = [
        "latitude_paciente",
        "longitude_paciente",
        "latitude_hospital",
        "longitude_hospital",
        "distancia_estimada_km",
    ]

    for coluna in campos_numericos:
        if coluna in fato.columns:
            fato[coluna] = fato[coluna].fillna(0)

    fato = fato.reset_index(drop=True)
    fato["internacao_id"] = range(1, len(fato) + 1)

    colunas_ordem = [
        "internacao_id",
        "id_hospital",
        "codigo_municipio_paciente",
        "tempo_id",
        "codigo_diagnostico",
        "data_internacao",
        "data_saida",
        "valor_procedimento",
        "dias_internacao",
        "paciente_viajou",
        "nome_municipio_paciente",
        "uf_paciente",
        "estado_paciente",
        "latitude_paciente",
        "longitude_paciente",
        "nome_municipio_hospital",
        "latitude_hospital",
        "longitude_hospital",
        "distancia_estimada_km",
        "ano_competencia",
        "mes_competencia",
        "dia_semana",
        "tipo_dia",
    ]

    colunas_existentes = [
        coluna for coluna in colunas_ordem if coluna in fato.columns
    ]

    fato = fato[colunas_existentes]

    logger.info("FATO_INTERNACAO gerada: %s registros", f"{len(fato):,}")
    return fato


def salvar_star_schema(
    dim_municipio,
    dim_hospital,
    dim_tempo,
    fato_internacao,
    uf=None,
    ano=None,
    mes=None,
    upload=True,
):
    """Salva as quatro tabelas finais do Star Schema."""
    uf = uf or config.UF
    ano = ano or config.ANO
    mes = mes or config.MES

    tabelas = {
        "dim_municipio": dim_municipio,
        "dim_hospital": dim_hospital,
        "dim_tempo": dim_tempo,
        "fato_internacao": fato_internacao,
    }

    resultados = {}

    for nome, df in tabelas.items():
        if df is None or df.empty:
            raise ValueError(f"{nome} está vazia e não pode ser salva.")

        caminho = config.PROCESSED_DIR / f"{nome}_{uf}_{ano}_{mes:02d}.parquet"

        df.to_parquet(caminho, index=False)
        resultados[nome] = caminho

        logger.info("%s salva: %s", nome.upper(), caminho)

        if upload:
            objeto = (
                f"{nome}/{uf}/{ano}/{mes:02d}/"
                f"{nome}_{uf}_{ano}_{mes:02d}.parquet"
            )

            if not upload_para_object_storage(caminho, objeto):
                raise RuntimeError(f"Falha no upload da tabela {nome}.")

    return resultados


def gerar_star_schema(
    df_sih,
    df_cnes,
    df_ibge,
    uf=None,
    ano=None,
    mes=None,
    salvar=True,
    upload=True,
):
    """Gera e, opcionalmente, salva o Star Schema completo."""
    uf = uf or config.UF
    ano = ano or config.ANO
    mes = mes or config.MES

    logger.info("GERANDO STAR SCHEMA PARA %s %s/%02d", uf, ano, mes)

    dim_municipio = gerar_dim_municipio(df_ibge, df_sih)
    dim_hospital = gerar_dim_hospital(df_cnes, dim_municipio)
    dim_tempo = gerar_dim_tempo(df_sih)

    fato_internacao = gerar_fato_internacao(
        df_sih,
        dim_hospital,
        dim_municipio,
        dim_tempo,
    )

    if salvar:
        salvar_star_schema(
            dim_municipio,
            dim_hospital,
            dim_tempo,
            fato_internacao,
            uf=uf,
            ano=ano,
            mes=mes,
            upload=upload,
        )

    return {
        "dim_municipio": dim_municipio,
        "dim_hospital": dim_hospital,
        "dim_tempo": dim_tempo,
        "fato_internacao": fato_internacao,
    }