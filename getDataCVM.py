from typing import Literal
import requests
import zipfile
import pandas as pd
from io import BytesIO


class DataCVM:
    def download_data(
        self, inicio: int, fim: int, url_base: str, zip_template: str, csv_template: str
    ) -> pd.DataFrame:
        """
        Baixa os dados para os anos especificados usando templates para o nome do ZIP e do CSV.
        """
        lista_dados = []
        for ano in range(inicio, fim):
            zip_filename = zip_template.format(ano=ano)
            url = url_base + zip_filename

            try:
                r = requests.get(url, timeout=10)
                r.raise_for_status()

                with zipfile.ZipFile(BytesIO(r.content)) as myzip:
                    csv_filename = csv_template.format(ano=ano)
                    with myzip.open(csv_filename) as file:
                        lines = file.readlines()
                        lines = [line.strip().decode("ISO-8859-1") for line in lines]
                        lines = [line.split(";") for line in lines]
                        df = pd.DataFrame(data=lines[1:], columns=lines[0])
                        lista_dados.append(df)
                        print(f"Leitura do ano {ano} finalizada.")

            except requests.exceptions.RequestException as e:
                print(f"Erro ao baixar dados de {ano}: {e}")
            except zipfile.BadZipFile:
                print(f"Erro ao descompactar o arquivo ZIP de {ano}.")
            except Exception as e:
                print(f"Erro inesperado no ano {ano}: {e}")

        return (
            pd.concat(lista_dados, ignore_index=True) if lista_dados else pd.DataFrame()
        )

    def obter_dados(
        self,
        dataset: str,  # Aqui, usaremos o Literal nas classes filhas
        inicio: int,
        fim: int,
    ) -> pd.DataFrame:
        """
        Método genérico para obter dados. As classes filhas devem definir:
          - self.url_base
          - self.zip_template
          - self.datasets (um dicionário com as chaves dos datasets disponíveis)

        Parâmetros:
          - dataset: nome do dataset a ser baixado.
          - inicio: ano inicial (inclusivo).
          - fim: ano final (exclusivo).
        """
        if not hasattr(self, "datasets"):
            raise AttributeError("Atributo 'datasets' não definido na classe.")
        if dataset not in self.datasets:
            raise ValueError(
                f"Dataset '{dataset}' não encontrado. Escolha entre: {list(self.datasets.keys())}"
            )
        csv_template = self.datasets[dataset]
        return self.download_data(
            inicio, fim, self.url_base, self.zip_template, csv_template
        )


class FCA(DataCVM):
    def __init__(self):
        self.url_base: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/"
        self.zip_template: str = "fca_cia_aberta_{ano}.zip"
        self.datasets: dict[str, str] = {
            "auditor": "fca_cia_aberta_auditor_{ano}.csv",
            "canal_divulgacao": "fca_cia_aberta_canal_divulgacao_{ano}.csv",
            "departamento_acionistas": "fca_cia_aberta_departamento_acionistas_{ano}.csv",
            "dri": "fca_cia_aberta_dri_{ano}.csv",
            "endereco": "fca_cia_aberta_endereco_{ano}.csv",
            "escriturador": "fca_cia_aberta_escriturador_{ano}.csv",
            "geral": "fca_cia_aberta_geral_{ano}.csv",
            "pais_estrangeiro_negociacao": "fca_cia_aberta_pais_estrangeiro_negociacao_{ano}.csv",
            "valor_mobiliario": "fca_cia_aberta_valor_mobiliario_{ano}.csv",
        }

    def obter_dados(
        self,
        dataset: Literal[
            "auditor",
            "canal_divulgacao",
            "departamento_acionistas",
            "dri",
            "endereco",
            "escriturador",
            "geral",
            "pais_estrangeiro_negociacao",
            "valor_mobiliario",
        ],
        inicio: int,
        fim: int,
    ) -> pd.DataFrame:
        """
        Obtém os dados do dataset desejado para os anos informados.
        """
        return super().obter_dados(dataset, inicio, fim)


class FRE(DataCVM):
    def __init__(self):
        self.url_base: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/"
        self.zip_template: str = "fre_cia_aberta_{ano}.zip"
        self.datasets: dict[str, str] = {
            "responsavel": "fre_cia_responsavel_{ano}.csv",
            "auditor": "fre_cia_auditor_{ano}.csv",
            "auditor_responsavel": "fre_cia_auditor_responsavel_{ano}.csv",
            "informacao_financeira": "fre_cia_informacao_financeira_{ano}.csv",
            "distribuicao_dividendos": "distribuicao_dividendos_{ano}.csv",
            "distribuicao_dividendos_classe_acao": "distribuicao_dividendos_classe_acao_{ano}.csv",
            "endividamento": "endividamento_{ano}.csv",
            "obrigacao": "obrigacao_{ano}.csv",
            "emissor": "emissor_{ano}.csv",
            "grupo_economico_reestruturacao": "grupo_economico_reestruturacao_{ano}.csv",
        }


class IPE(DataCVM):
    def __init__(self):
        self.url_base = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/"


class ITR(DataCVM):
    def __init__(self):
        self.url_base = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"


class VLMO(DataCVM):
    def __init__(self):
        self.url_base = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/VLMO/DADOS/"


class ICBGC(DataCVM):
    def __init__(self):
        self.url_base = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ICBGC/DADOS/"


class DFP(DataCVM):
    def __init__(self):
        self.url_base = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"


class DadosCadastrais(DataCVM):
    def __init__(self):
        self.url = (
            "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"
        )
