"""
transformacao.py - Limpeza e padronizacao das fontes de dados do MedData.
- SIH/SUS: Limpeza, padronizacao de datas, codigos, criacao de features
- CNES: Padronizacao de colunas, agregacao por hospital
- IBGE: Padronizacao de codigos, selecao de colunas, adicao de UF e estado

"""

'''Nesse código utilizamos a analise exploratoria (arquivos EDA) para conhecer
 os dados e saber como nós queriamso para nosso trabalho: escolha de colunas, criação de features, tratamento e etc.
 Por isso, a grande maioria deste código irá envolver o conteúdo da matéria de estatistica em script'''


import argparse
import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np


# Adiciona src ao path
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent))


from config import config


#colocamos para a viasualizarmos no terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)




ESTADOS = {
    'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapa', 'AM': 'Amazonas',
    'BA': 'Bahia', 'CE': 'Ceara', 'DF': 'Distrito Federal', 'ES': 'Espirito Santo',
    'GO': 'Goias', 'MA': 'Maranhao', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais', 'PA': 'Para', 'PB': 'Paraiba', 'PR': 'Parana',
    'PE': 'Pernambuco', 'PI': 'Piaui', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul', 'RO': 'Rondonia', 'RR': 'Roraima', 'SC': 'Santa Catarina',
    'SP': 'Sao Paulo', 'SE': 'Sergipe', 'TO': 'Tocantins'
}


def transformar_sih(df, uf=None, ano=None, mes=None):
    """Limpa e padroniza os dados brutos do SIH/SUS."""
    uf = uf or config.UF
    ano = ano or config.ANO
    mes = mes or config.MES

    logger.info(f"Transformando SIH: {len(df):,} registros para {uf} {ano}/{mes:02d}")

    # Mapeia as colunas reais do SIH para nomes padronizados do projeto
    mapeamento = {
        'MUNIC_MOV': 'codigo_municipio',
        'CNES': 'id_hospital',
        'DIAG_PRINC': 'codigo_diagnostico',
        'DT_INTER': 'data_internacao',
        'DT_SAIDA': 'data_saida',
        'VAL_TOT': 'valor_procedimento',
        'MUNIC_RES': 'codigo_municipio_paciente',
        'ANO_CMPT': 'ano_competencia',
        'MES_CMPT': 'mes_competencia'
    }

    colunas_existentes = [
        coluna for coluna in mapeamento
        if coluna in df.columns
    ]

    df_clean = df[colunas_existentes].copy()
    df_clean.rename(
        columns={coluna: mapeamento[coluna] for coluna in colunas_existentes},
        inplace=True
    )

    # Conversão de datas
    for coluna in ['data_internacao', 'data_saida']:
        if coluna in df_clean.columns:
            df_clean[coluna] = pd.to_datetime(
                df_clean[coluna],
                format='%Y%m%d',
                errors='coerce'
            )

    # Conversão de valores numéricos
    if 'valor_procedimento' in df_clean.columns:
        df_clean['valor_procedimento'] = pd.to_numeric(
            df_clean['valor_procedimento'],
            errors='coerce'
        ).fillna(0)

    for coluna in ['ano_competencia', 'mes_competencia']:
        if coluna in df_clean.columns:
            df_clean[coluna] = pd.to_numeric(
                df_clean[coluna],
                errors='coerce'
            )

    # Padronização dos códigos
    if 'id_hospital' in df_clean.columns:
        df_clean['id_hospital'] = (
            df_clean['id_hospital']
            .astype(str)
            .str.replace('.0', '', regex=False)
            .str.zfill(7)
        )

    for coluna in ['codigo_municipio', 'codigo_municipio_paciente']:
        if coluna in df_clean.columns:
            df_clean[coluna] = (
                df_clean[coluna]
                .astype(str)
                .str.replace('.0', '', regex=False)
                .str.zfill(6)
            )

    # Features derivadas
    if {'data_internacao', 'data_saida'}.issubset(df_clean.columns):
        df_clean['dias_internacao'] = (
            df_clean['data_saida'] - df_clean['data_internacao']
        ).dt.days.fillna(0).clip(lower=0).astype(int)

    if {'codigo_municipio', 'codigo_municipio_paciente'}.issubset(df_clean.columns):
        df_clean['paciente_viajou'] = (
            df_clean['codigo_municipio_paciente']
            != df_clean['codigo_municipio']
        )

    if 'data_internacao' in df_clean.columns:
        df_clean['dia_semana'] = df_clean['data_internacao'].dt.day_name()
        df_clean['tipo_dia'] = df_clean['dia_semana'].apply(
            lambda dia: 'Fim de Semana'
            if dia in ['Saturday', 'Sunday']
            else 'Dia Util'
        )

    if {'ano_competencia', 'mes_competencia'}.issubset(df_clean.columns):
        df_clean['ano_mes'] = (
            df_clean['ano_competencia'].astype('Int64').astype(str)
            + '-'
            + df_clean['mes_competencia'].astype('Int64').astype(str).str.zfill(2)
        )

    # Limpeza final
    df_clean = df_clean.drop_duplicates()

    if 'data_internacao' in df_clean.columns:
        df_clean = df_clean[df_clean['data_internacao'].notna()]

    if 'id_hospital' in df_clean.columns:
        df_clean = df_clean[df_clean['id_hospital'].notna()]

    logger.info(f"SIH transformado: {len(df_clean):,} registros")
    return df_clean


def transformar_cnes(df, uf=None, ano=None, mes=None):
    """Limpa e padroniza os dados brutos do CNES."""
    uf = uf or config.UF
    ano = ano or config.ANO
    mes = mes or config.MES

    logger.info(f"Transformando CNES: {len(df):,} registros para {uf} {ano}/{mes:02d}")

    df.columns = [col.upper() for col in df.columns]

    colunas_cnes = {
        'CNES': 'id_hospital',
        'CODUFMUN': 'codigo_municipio',
        'TP_LEITO': 'tipo_leito',
        'QT_EXIST': 'leitos_totais',
        'QT_SUS': 'leitos_sus',
        'QT_NSUS': 'leitos_nao_sus',
        'ESFERA_A': 'esfera',
        'TP_UNID': 'tipo_unidade',
        'NIV_HIER': 'nivel_hierarquico',
        'NAT_JUR': 'natureza_juridica'
    }

    colunas_existentes = [col for col in colunas_cnes.keys() if col in df.columns]
    df_clean = df[colunas_existentes].copy()
    df_clean.rename(columns={k: v for k, v in colunas_cnes.items() if k in df.columns}, inplace=True)


    colunas_numericas = ['leitos_totais', 'leitos_sus', 'leitos_nao_sus']
    for col in colunas_numericas:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0).astype(int)


    if 'id_hospital' in df_clean.columns:
        df_clean['id_hospital'] = df_clean['id_hospital'].astype(str).str.zfill(7)
    if 'codigo_municipio' in df_clean.columns:
        df_clean['codigo_municipio'] = df_clean['codigo_municipio'].astype(str).str.zfill(6)


    colunas_agrupar = [
        'id_hospital', 'codigo_municipio', 'esfera',
        'tipo_unidade', 'nivel_hierarquico', 'natureza_juridica'
    ]
    colunas_agrupar = [col for col in colunas_agrupar if col in df_clean.columns]


    colunas_somar = [col for col in colunas_numericas if col in df_clean.columns]


    df_agrupado = df_clean.groupby(colunas_agrupar)[colunas_somar].sum().reset_index()


    def classificar_porte(total_leitos):
        if total_leitos >= 150:
            return 'Grande Porte'
        elif total_leitos >= 50:
            return 'Medio Porte'
        elif total_leitos >= 15:
            return 'Pequeno Porte'
        return 'Micro Porte'


    if 'leitos_totais' in df_agrupado.columns:
        df_agrupado['porte_hospitalar'] = df_agrupado['leitos_totais'].apply(classificar_porte)


    if 'nivel_hierarquico' in df_agrupado.columns:
        df_agrupado['alta_complexidade'] = df_agrupado['nivel_hierarquico'].apply(
            lambda x: 'Alta Complexidade' if pd.notna(x) and x != '' else 'Baixa/Media Complexidade'
        )


    if 'leitos_totais' in df_agrupado.columns and 'leitos_sus' in df_agrupado.columns:
        df_agrupado['percentual_sus'] = (
            df_agrupado['leitos_sus'] / df_agrupado['leitos_totais'] * 100
        ).fillna(0).clip(0, 100).round(2)


    df_agrupado = df_agrupado.drop_duplicates(subset=['id_hospital'])


    logger.info(f"CNES transformado: {len(df_agrupado):,} hospitais")
    return df_agrupado


def transformar_ibge(df, uf=None):
    """Limpa e padroniza os dados do IBGE, adicionando UF e estado."""
    uf = uf or config.UF


    logger.info(f"Transformando IBGE: {len(df):,} municipios para {uf}")


    colunas_ibge = ['codigo_ibge', 'nome', 'uf', 'latitude', 'longitude']
    colunas_existentes = [col for col in colunas_ibge if col in df.columns]
    logger.info(f"Colunas encontradas: {colunas_existentes}")


    df_clean = df[colunas_existentes].copy()


    df_clean.rename(columns={
        'codigo_ibge': 'codigo_municipio',
        'nome': 'nome_municipio',
        'uf': 'uf',
        'latitude': 'latitude',
        'longitude': 'longitude'
    }, inplace=True)


    df_clean['estado'] = df_clean['uf'].map(ESTADOS).fillna('Nao informado')
    df_clean['nome_municipio'] = df_clean['nome_municipio'].fillna('Nao informado')
    df_clean['latitude'] = df_clean['latitude'].fillna(0)
    df_clean['longitude'] = df_clean['longitude'].fillna(0)
    df_clean['codigo_municipio'] = df_clean['codigo_municipio'].astype(str).str[:6]


    logger.info(f"IBGE transformado: {len(df_clean):,} municipios")
    return df_clean


def salvar_resultados_transformacao(df_sih, df_cnes, df_ibge, uf=None, ano=None, mes=None, upload=True):
    """Salva dataFrames transformados localmente e no object storage."""
    uf = uf or config.UF
    ano = ano or config.ANO
    mes = mes or config.MES


    resultados = {}


    if df_sih is not None and len(df_sih) > 0:
        caminho = config.PROCESSED_DIR / f"sih_transformado_{uf}_{ano}_{mes:02d}.parquet"
        df_sih.to_parquet(caminho, index=False)
        resultados['sih'] = caminho
        logger.info(f"SIH salvo: {caminho} ({len(df_sih):,} registros)")


        if upload:
            objeto = f"sih_transformado/{uf}/{ano}/{mes:02d}/sih_transformado_{uf}_{ano}_{mes:02d}.parquet"
            _upload_para_object_storage(caminho, objeto, "meddata-gold")


    if df_cnes is not None and len(df_cnes) > 0:
        caminho = config.PROCESSED_DIR / f"cnes_transformado_{uf}_{ano}_{mes:02d}.parquet"
        df_cnes.to_parquet(caminho, index=False)
        resultados['cnes'] = caminho
        logger.info(f"CNES salvo: {caminho} ({len(df_cnes):,} registros)")


        if upload:
            objeto = f"cnes_transformado/{uf}/{ano}/{mes:02d}/cnes_transformado_{uf}_{ano}_{mes:02d}.parquet"
            _upload_para_object_storage(caminho, objeto, "meddata-gold")


    if df_ibge is not None and len(df_ibge) > 0:
        caminho = config.PROCESSED_DIR / f"ibge_transformado_{uf}.parquet"
        df_ibge.to_parquet(caminho, index=False)
        resultados['ibge'] = caminho
        logger.info(f"IBGE salvo: {caminho} ({len(df_ibge):,} registros)")


        if upload:
            objeto = f"ibge_transformado/{uf}/ibge_transformado_{uf}.parquet"
            _upload_para_object_storage(caminho, objeto, "meddata-gold")


    return resultados




def _upload_para_object_storage(arquivo_local, objeto_name, bucket="meddata-gold"):
    """Faz upload de um arquivo para o Object Storage da OCI."""
    try:
        import oci
        from oci.config import from_file


        config_oci = from_file()
        object_storage = oci.object_storage.ObjectStorageClient(config_oci)
        namespace = object_storage.get_namespace().data


        with open(arquivo_local, "rb") as arquivo:
            object_storage.put_object(namespace, bucket, objeto_name, arquivo)


        logger.info(f"Upload concluido: {bucket}/{objeto_name}")
        return True
    except Exception as e:
        logger.error(f"Falha no upload: {str(e)}")
        return False




def executar_transformacao(df_sih_raw, df_cnes_raw, df_ibge_raw, uf=None, ano=None, mes=None, upload=True):
    """Executa o pipeline de transformacao das tres fontes de dados."""
    uf = uf or config.UF
    ano = ano or config.ANO
    mes = mes or config.MES


    logger.info("Iniciando transformacao para %s %d/%02d", uf, ano, mes)


    df_sih = transformar_sih(df_sih_raw, uf, ano, mes)
    df_cnes = transformar_cnes(df_cnes_raw, uf, ano, mes)
    df_ibge = transformar_ibge(df_ibge_raw, uf)


    salvar_resultados_transformacao(df_sih, df_cnes, df_ibge, uf, ano, mes, upload)


    return {'sih': df_sih, 'cnes': df_cnes, 'ibge': df_ibge}




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transformacao de dados do MedData")
    parser.add_argument('--uf', type=str, default=config.UF, help='UF do estado')
    parser.add_argument('--ano', type=int, default=config.ANO, help='Ano dos dados')
    parser.add_argument('--mes', type=int, default=config.MES, help='Mes dos dados')
    parser.add_argument('--upload', action='store_true', default=True, help='Fazer upload para OCI')
    parser.add_argument('--no-upload', action='store_false', dest='upload', help='Nao fazer upload para OCI')
    args = parser.parse_args()


    print("=" * 60)
    print("INICIANDO TRANSFORMACAO DE DADOS")
    print(f"UF: {args.uf} | Ano: {args.ano} | Mes: {args.mes:02d}")
    print("=" * 60)


    config.UF = args.uf
    config.ANO = args.ano
    config.MES = args.mes


    from ingestao import baixar_sih, baixar_cnes_leitos, baixar_ibge


    df_sih_raw = baixar_sih(args.uf, args.ano, args.mes, upload=False)
    df_cnes_raw = baixar_cnes_leitos(args.uf, args.ano, args.mes, upload=False)
    df_ibge_raw = baixar_ibge(args.uf, upload=False)


    if df_sih_raw is None or df_cnes_raw is None or df_ibge_raw is None:
        print("Falha ao carregar dados brutos.")
        sys.exit(1)


    executar_transformacao(
        df_sih_raw=df_sih_raw,
        df_cnes_raw=df_cnes_raw,
        df_ibge_raw=df_ibge_raw,
        uf=args.uf,
        ano=args.ano,
        mes=args.mes,
        upload=args.upload
    )

    print("\nTransformacao concluida com sucesso.")
    sys.exit(0)