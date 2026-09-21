"""
validacao.py - Validação do Star Schema do MedData.
"""

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).parent))

from config import config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def carregar_tabelas(uf=None, ano=None, mes=None):
    """Carrega os quatro arquivos finais do Star Schema."""
    uf = uf or config.UF
    ano = ano or config.ANO
    mes = mes or config.MES

    caminhos = {
        "dim_municipio": (
            config.PROCESSED_DIR
            / f"dim_municipio_{uf}_{ano}_{mes:02d}.parquet"
        ),
        "dim_hospital": (
            config.PROCESSED_DIR
            / f"dim_hospital_{uf}_{ano}_{mes:02d}.parquet"
        ),
        "dim_tempo": (
            config.PROCESSED_DIR
            / f"dim_tempo_{uf}_{ano}_{mes:02d}.parquet"
        ),
        "fato_internacao": (
            config.PROCESSED_DIR
            / f"fato_internacao_{uf}_{ano}_{mes:02d}.parquet"
        ),
    }

    dados = {}

    for nome, caminho in caminhos.items():
        if not caminho.exists():
            logger.error("Arquivo não encontrado: %s", caminho)
            dados[nome] = None
            continue

        dados[nome] = pd.read_parquet(caminho)
        logger.info(
            "Carregado: %s (%s registros)",
            nome,
            f"{len(dados[nome]):,}",
        )

    return dados


def validar_chaves_primarias(dim_municipio, dim_hospital, dim_tempo, fato):
    """Verifica duplicidades nas chaves primárias."""
    erros = []

    verificacoes = [
        ("DIM_MUNICIPIO", dim_municipio, "codigo_municipio"),
        ("DIM_HOSPITAL", dim_hospital, "id_hospital"),
        ("DIM_TEMPO", dim_tempo, "tempo_id"),
        ("FATO_INTERNACAO", fato, "internacao_id"),
    ]

    for nome_tabela, df, coluna in verificacoes:
        if df is not None and coluna in df.columns:
            duplicados = df[coluna].duplicated().sum()

            if duplicados > 0:
                erros.append(
                    f"{nome_tabela}: {duplicados} valores duplicados em {coluna}"
                )

    return erros


def validar_integridade_referencial(fato, dim_hospital, dim_municipio, dim_tempo):
    """Verifica se as chaves estrangeiras existem nas dimensões."""
    erros = []

    if fato is None:
        return ["FATO_INTERNACAO não foi carregada."]

    if dim_hospital is not None:
        hospitais_invalidos = (
            set(fato["id_hospital"]) - set(dim_hospital["id_hospital"])
        )

        if hospitais_invalidos:
            erros.append(
                f"FK_HOSPITAL: {len(hospitais_invalidos)} hospitais sem correspondência"
            )

    if dim_tempo is not None:
        tempos_invalidos = (
            set(fato["tempo_id"]) - set(dim_tempo["tempo_id"])
        )

        if tempos_invalidos:
            erros.append(
                f"FK_TEMPO: {len(tempos_invalidos)} datas sem correspondência"
            )

    if dim_municipio is not None:
        municipios_invalidos = (
            set(fato["codigo_municipio_paciente"])
            - set(dim_municipio["codigo_municipio"])
        )

        if municipios_invalidos:
            erros.append(
                "FK_MUNICIPIO: "
                f"{len(municipios_invalidos)} municípios de paciente sem correspondência"
            )

    return erros


def validar_nulos(dim_municipio, dim_hospital, dim_tempo, fato):
    """Verifica nulos em colunas obrigatórias."""
    erros = []
    avisos = []

    regras = {
        "DIM_MUNICIPIO": (
            dim_municipio,
            ["codigo_municipio", "nome_municipio", "uf"],
        ),
        "DIM_HOSPITAL": (
            dim_hospital,
            ["id_hospital", "leitos_totais"],
        ),
        "DIM_TEMPO": (
            dim_tempo,
            ["tempo_id", "data_referencia", "ano", "mes"],
        ),
        "FATO_INTERNACAO": (
            fato,
            ["internacao_id", "id_hospital", "data_internacao", "tempo_id"],
        ),
    }

    for nome_tabela, (df, colunas) in regras.items():
        if df is None:
            continue

        for coluna in colunas:
            if coluna in df.columns:
                total_nulos = df[coluna].isna().sum()

                if total_nulos > 0:
                    erros.append(
                        f"{nome_tabela}: {total_nulos} nulos em {coluna}"
                    )

    if fato is not None:
        for coluna in ["data_saida", "codigo_diagnostico"]:
            if coluna not in fato.columns or fato.empty:
                continue

            total_nulos = fato[coluna].isna().sum()
            percentual = total_nulos / len(fato) * 100

            if percentual > 50:
                avisos.append(
                    f"FATO_INTERNACAO: {percentual:.1f}% de nulos em {coluna}"
                )

    return erros, avisos


def validar_consistencia(dim_municipio, dim_hospital, fato):
    """Valida faixas numéricas e regras de negócio básicas."""
    erros = []

    if dim_municipio is not None and "latitude" in dim_municipio.columns:
        invalidas = dim_municipio[
            (dim_municipio["latitude"] < -90)
            | (dim_municipio["latitude"] > 90)
        ]

        if not invalidas.empty:
            erros.append(
                f"DIM_MUNICIPIO: {len(invalidas)} registros com latitude inválida"
            )

    if dim_hospital is not None and "leitos_totais" in dim_hospital.columns:
        negativos = dim_hospital[dim_hospital["leitos_totais"] < 0]

        if not negativos.empty:
            erros.append(
                f"DIM_HOSPITAL: {len(negativos)} registros com leitos negativos"
            )

    if dim_hospital is not None and "percentual_sus" in dim_hospital.columns:
        invalidos = dim_hospital[
            (dim_hospital["percentual_sus"] < 0)
            | (dim_hospital["percentual_sus"] > 100)
        ]

        if not invalidos.empty:
            erros.append(
                "DIM_HOSPITAL: "
                f"{len(invalidos)} registros com percentual SUS inválido"
            )

    if fato is not None and "dias_internacao" in fato.columns:
        negativos = fato[fato["dias_internacao"] < 0]

        if not negativos.empty:
            erros.append(
                f"FATO_INTERNACAO: {len(negativos)} registros com dias negativos"
            )

    return erros


def gerar_relatorio(dados, erros, avisos):
    """Exibe um resumo da validação nos logs do Airflow."""
    logger.info("=" * 60)
    logger.info("RELATÓRIO DE VALIDAÇÃO")
    logger.info("=" * 60)

    for nome, df in dados.items():
        if df is not None:
            logger.info(
                "%s | %s registros | %s colunas",
                nome.upper(),
                f"{len(df):,}",
                len(df.columns),
            )

    if erros:
        logger.error("VALIDAÇÃO REPROVADA: %s erro(s)", len(erros))

        for erro in erros:
            logger.error("- %s", erro)

        return False

    logger.info("VALIDAÇÃO APROVADA: nenhum erro encontrado.")

    for aviso in avisos:
        logger.warning("- %s", aviso)

    return True


def executar_validacao(uf=None, ano=None, mes=None):
    """Executa todas as validações do Star Schema."""
    uf = uf or config.UF
    ano = ano or config.ANO
    mes = mes or config.MES

    logger.info("INICIANDO VALIDAÇÃO: %s %s/%02d", uf, ano, mes)

    dados = carregar_tabelas(uf, ano, mes)

    if any(df is None for df in dados.values()):
        logger.error("Uma ou mais tabelas não foram encontradas.")
        return False

    if any(df.empty for df in dados.values()):
        logger.error("Uma ou mais tabelas estão vazias.")
        return False

    erros = []
    avisos = []

    erros.extend(
        validar_chaves_primarias(
            dados["dim_municipio"],
            dados["dim_hospital"],
            dados["dim_tempo"],
            dados["fato_internacao"],
        )
    )

    erros.extend(
        validar_integridade_referencial(
            dados["fato_internacao"],
            dados["dim_hospital"],
            dados["dim_municipio"],
            dados["dim_tempo"],
        )
    )

    erros_nulos, avisos_nulos = validar_nulos(
        dados["dim_municipio"],
        dados["dim_hospital"],
        dados["dim_tempo"],
        dados["fato_internacao"],
    )

    erros.extend(erros_nulos)
    avisos.extend(avisos_nulos)

    erros.extend(
        validar_consistencia(
            dados["dim_municipio"],
            dados["dim_hospital"],
            dados["fato_internacao"],
        )
    )

    return gerar_relatorio(dados, erros, avisos)