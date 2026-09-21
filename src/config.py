from pathlib import Path
import os


class Config:
    """Configuracao central do projeto MedData."""

    def __init__(self):
        self.UF = "SP"
        self.ANO = 2024
        self.MES = 1

        # No Airflow: /opt/airflow/data
        # Localmente, pode definir MEDDATA_BASE_DIR se necessário.
        self.BASE_DIR = Path(
            os.getenv("MEDDATA_BASE_DIR", "/opt/airflow")
        )

        self.DATA_DIR = self.BASE_DIR / "data"
        self.RAW_DIR = self.DATA_DIR / "raw"
        self.PROCESSED_DIR = self.DATA_DIR / "processado"
        self.REFERENCE_DIR = self.DATA_DIR / "reference"

        for pasta in [self.RAW_DIR, self.PROCESSED_DIR, self.REFERENCE_DIR]:
            pasta.mkdir(parents=True, exist_ok=True)

        self.CODIGOS_UF = {
            "PR": 41, "SP": 35, "RJ": 33, "MG": 31,
            "BA": 29, "RS": 43, "SC": 42, "GO": 52,
            "PE": 26, "CE": 23, "PA": 15, "AM": 13,
            "MT": 51, "MS": 50, "DF": 53, "ES": 32,
            "MA": 21, "RN": 24, "PB": 25, "PI": 22,
            "AL": 27, "SE": 28, "TO": 17, "RO": 11,
            "AC": 12, "AP": 16, "RR": 14
        }

    def get_codigo_uf(self, uf=None):
        uf = (uf or self.UF).upper()
        return self.CODIGOS_UF.get(uf)

    def get_nome_arquivo(self, prefixo, uf=None, ano=None, mes=None,
                         extensao="parquet"):
        uf = uf or self.UF

        # IBGE não depende de ano/mês
        if prefixo == "ibge":
            return f"ibge_{uf}.{extensao}"

        ano = ano or self.ANO
        mes = mes or self.MES
        return f"{prefixo}_{uf}_{ano}_{mes:02d}.{extensao}"


config = Config()