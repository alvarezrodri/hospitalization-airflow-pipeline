"""
meddata_pipeline.py - DAG do Airflow para o MedData
"""

import sys
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

# Permite importar os arquivos da pasta src
sys.path.append("/opt/airflow/src")

from config import config
from ingestao import baixar_todos
from transformacao import executar_transformacao
from integracao import gerar_star_schema
from validacao import executar_validacao
from carga_analitica import carregar_star_schema
# Se o nome real for upload_to_adb.py, use:
# from upload_to_adb import carregar_star_schema


default_args = {
    "owner": "meddata",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def tarefa_ingestao(**context):
    """Baixa e salva os dados brutos nas camadas raw e reference."""
    mes = context["logical_date"].month

    print(f"[INGESTAO] Baixando dados para SP/2024/{mes:02d}")

    resultados = baixar_todos(
        uf="SP",
        ano=2024,
        mes=mes,
        upload=False,
    )

    if not all(df is not None for df in resultados.values()):
        raise RuntimeError("Falha na ingestão de uma ou mais fontes.")

    print("[INGESTAO] Concluída com sucesso.")


def tarefa_transformacao(**context):
    """Lê os dados brutos, transforma e salva os Parquets tratados."""
    mes = context["logical_date"].month

    print(f"[TRANSFORMACAO] Transformando dados para SP/2024/{mes:02d}")

    df_sih = pd.read_parquet(
        config.RAW_DIR / f"sih_SP_2024_{mes:02d}.parquet"
    )
    df_cnes = pd.read_parquet(
        config.RAW_DIR / f"cnes_SP_2024_{mes:02d}.parquet"
    )
    df_ibge = pd.read_parquet(
        config.REFERENCE_DIR / "ibge_SP.parquet"
    )

    executar_transformacao(
        df_sih_raw=df_sih,
        df_cnes_raw=df_cnes,
        df_ibge_raw=df_ibge,
        uf="SP",
        ano=2024,
        mes=mes,
        upload=False,
    )

    print("[TRANSFORMACAO] Concluída com sucesso.")


def tarefa_integracao(**context):
    """Gera e salva as dimensões e a tabela fato do Star Schema."""
    mes = context["logical_date"].month

    print(f"[INTEGRACAO] Gerando Star Schema para SP/2024/{mes:02d}")

    df_sih = pd.read_parquet(
        config.PROCESSED_DIR / f"sih_transformado_SP_2024_{mes:02d}.parquet"
    )
    df_cnes = pd.read_parquet(
        config.PROCESSED_DIR / f"cnes_transformado_SP_2024_{mes:02d}.parquet"
    )
    df_ibge = pd.read_parquet(
        config.PROCESSED_DIR / "ibge_transformado_SP.parquet"
    )

    gerar_star_schema(
        df_sih=df_sih,
        df_cnes=df_cnes,
        df_ibge=df_ibge,
        uf="SP",
        ano=2024,
        mes=mes,
        salvar=True,
        upload=True,
    )

    print("[INTEGRACAO] Concluída com sucesso.")


def tarefa_validacao(**context):
    """Valida qualidade, chaves e integridade das tabelas do Star Schema."""
    mes = context["logical_date"].month

    print(f"[VALIDACAO] Validando dados para SP/2024/{mes:02d}")

    valido = executar_validacao(
        uf="SP",
        ano=2024,
        mes=mes,
    )

    if not valido:
        raise ValueError("Validação falhou. Consulte os logs da tarefa.")

    print("[VALIDACAO] Dados aprovados para carga.")


def tarefa_carga(**context):
    """Carrega o Star Schema no Oracle Autonomous Database."""
    mes = context["logical_date"].month

    print(f"[CARGA] Carregando dados para SP/2024/{mes:02d}")

    conn = BaseHook.get_connection("oracle_adb")

    sucesso = carregar_star_schema(
        uf="SP",
        ano=2024,
        mes=mes,
        user=conn.login,
        password=conn.password,
        dsn="meddata_high",
        namespace="grtrhsatn1p6",
        credential_name="MEDDATA_CRED",
    )

    if not sucesso:
        raise RuntimeError("Falha na carga analítica no Autonomous Database.")

    print("[CARGA] Concluída com sucesso.")


with DAG(
    dag_id="meddata_pipeline",
    default_args=default_args,
    description="Pipeline de dados MedData: SIH, CNES, IBGE e Oracle ADB",
    schedule="0 0 5 * *",
    catchup=False,
    max_active_runs=1,
    tags=["meddata", "sih", "cnes", "oracle"],
) as dag:

    t1 = PythonOperator(
        task_id="ingestao",
        python_callable=tarefa_ingestao,
        do_xcom_push=False,
    )

    t2 = PythonOperator(
        task_id="transformacao",
        python_callable=tarefa_transformacao,
        do_xcom_push=False,
    )

    t3 = PythonOperator(
        task_id="integracao",
        python_callable=tarefa_integracao,
        do_xcom_push=False,
    )

    t4 = PythonOperator(
        task_id="validacao",
        python_callable=tarefa_validacao,
        do_xcom_push=False,
    )

    t5 = PythonOperator(
        task_id="carga_analitica",
        python_callable=tarefa_carga,
        do_xcom_push=False,
    )

    t1 >> t2 >> t3 >> t4 >> t5