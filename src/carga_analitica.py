
"""
upload_to_adb.py - Carga analitica no Autonomous Database.

Este script carrega os arquivos Parquet do Object Storage
para as tabelas do Autonomous Database usando DBMS_CLOUD.COPY_DATA.
"""


import oracledb
import argparse
import sys
from pathlib import Path
import os

sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent))

from config import config


def conectar_adb(user, password, dsn):
    """Conecta ao Autonomous Database usando a Wallet."""
    try:
        wallet_dir = "/opt/airflow/wallet"
        wallet_password = os.getenv("WALLET_PASSWORD")

        if not wallet_password:
            raise RuntimeError("WALLET_PASSWORD não foi configurada.")
        
        conn = oracledb.connect(
            user=user,
            password=password,
            dsn=dsn,
            config_dir=wallet_dir,
            wallet_location=wallet_dir,
            wallet_password=wallet_password,
            tcp_connect_timeout=10,
            retry_count=1,
            retry_delay=1,
        )

        print(f"Conectado ao Autonomous Database como {user}")
        return conn

    except Exception as e:
        print(f"Erro na conexao: {e}")
        return None
    


def carregar_tabela(conn, table_name, arquivo_parquet, uf, ano, mes, namespace, credential_name):
    """
    Carrega um arquivo Parquet para uma tabela no ADB.
    """
    cursor = conn.cursor()
    
    url = f"https://objectstorage.sa-saopaulo-1.oraclecloud.com/n/{namespace}/b/meddata-gold/o/{table_name.lower()}/{uf}/{ano}/{mes:02d}/{arquivo_parquet}"
    
    print(f"\nCarregando {table_name}...")
    print(f"  Arquivo: {arquivo_parquet}")
    
    sql = f"""
    BEGIN
        DBMS_CLOUD.COPY_DATA(
            table_name => '{table_name}',
            credential_name => '{credential_name}',
            file_uri_list => '{url}',
            format => json_object('type' value 'parquet')
        );
    END;
    """
    
    try:
        cursor.execute(sql)
        conn.commit()
        print(f"  {table_name} carregada com sucesso")
        return True
    except Exception as e:
        print(f"  Erro ao carregar {table_name}: {e}")
        return False
    finally:
        cursor.close()


def carregar_star_schema(uf, ano, mes, user, password, dsn, namespace, credential_name):
    """
    Carrega todas as tabelas do Star Schema.
    """
    print("=" * 60)
    print("INICIANDO CARGA ANALITICA")
    print("=" * 60)
    print(f"UF: {uf}")
    print(f"Ano: {ano}")
    print(f"Mes: {mes:02d}")
    print(f"DSN: {dsn}")
    print(f"Namespace: {namespace}")
    print("=" * 60)
    
    conn = conectar_adb(user, password, dsn)
    if conn is None:
        print("Falha na conexao com o banco. Abortando...")
        return False
    
    tabelas = [
        ('DIM_MUNICIPIO', f'dim_municipio_{uf}_{ano}_{mes:02d}.parquet'),
        ('DIM_HOSPITAL', f'dim_hospital_{uf}_{ano}_{mes:02d}.parquet'),
        ('DIM_TEMPO', f'dim_tempo_{uf}_{ano}_{mes:02d}.parquet'),
        ('FATO_INTERNACAO', f'fato_internacao_{uf}_{ano}_{mes:02d}.parquet'),
    ]
    
    sucessos = 0
    for table_name, arquivo in tabelas:
        if carregar_tabela(conn, table_name, arquivo, uf, ano, mes, namespace, credential_name):
            sucessos += 1
    
    conn.close()
    
    print("\n" + "=" * 60)
    if sucessos == len(tabelas):
        print(f"CARGA CONCLUIDA COM SUCESSO ({sucessos}/{len(tabelas)} tabelas)")
    else:
        print(f"CARGA PARCIAL: {sucessos}/{len(tabelas)} tabelas carregadas")
    print("=" * 60)
    
    return sucessos == len(tabelas)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Carga analitica no Autonomous Database",
        epilog="""
Exemplo de uso:
  python upload_to_adb.py --uf SP --ano 2024 --mes 1 \\
      --user ADMIN --password SuaSenha --dsn meddatadb_high \\
      --namespace axg123456789 --credential MEDDATA_CRED
        """
    )
    parser.add_argument('--uf', type=str, required=True, help='UF do estado')
    parser.add_argument('--ano', type=int, required=True, help='Ano dos dados')
    parser.add_argument('--mes', type=int, required=True, help='Mes dos dados')
    parser.add_argument('--user', type=str, required=True, help='Usuario do Autonomous Database')
    parser.add_argument('--password', type=str, required=True, help='Senha do Autonomous Database')
    parser.add_argument('--dsn', type=str, required=True, help='DSN do Autonomous Database')
    parser.add_argument('--namespace', type=str, required=True, help='Namespace do Object Storage')
    parser.add_argument('--credential', type=str, default='MEDDATA_CRED', 
                        help='Nome da credential no ADB (padrao: MEDDATA_CRED)')
    
    args = parser.parse_args()
    
    if args.mes < 1 or args.mes > 12:
        print("Mes invalido. Deve ser entre 1 e 12.")
        sys.exit(1)
    
    sucesso = carregar_star_schema(
        uf=args.uf,
        ano=args.ano,
        mes=args.mes,
        user=args.user,
        password=args.password,
        dsn=args.dsn,
        namespace=args.namespace,
        credential_name=args.credential
    )
    
    sys.exit(0 if sucesso else 1)