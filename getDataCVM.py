import requests
import zipfile
import pandas as pd
from io import BytesIO
from typing import Literal
from bs4 import BeautifulSoup


class DataCVM:
    def find_dataset(self, tipo):

        if "fca" in tipo:
            limitador = "("
        elif "fre" in tipo:
            limitador = ":"
        r = requests.get(self.url_dataset)
        html = BeautifulSoup(r.text, "html.parser")
        li_strong = [li for li in html.find_all("li") if li.find("strong")]
        dataset = dict()
        for li in li_strong:
            texto = li.get_text(strip=True)

            if texto.startswith(tipo.removesuffix("aberta")):

                limite_superior = texto.find(limitador)
                texto = texto[:limite_superior].replace("(anteriormente", "")

                if tipo in texto:
                    chave = texto.removeprefix(tipo + "_")
                    valor = f"{texto}_" + "{ano}.csv"

                else:
                    chave = texto.removeprefix(tipo.removesuffix("aberta"))
                    valor = f"{tipo}_{chave}_" + "{ano}.csv"

                dataset[chave] = valor

        # return dict(sorted(dataset.items()))
        return dataset

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

    def get_data(
        self,
        dataset: list[str],  # Aqui, usaremos o Literal nas classes filhas
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
        self.url_dataset: str = "https://dados.cvm.gov.br/dataset/cia_aberta-doc-fca"
        self.url_base: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/"
        self.zip_template: str = "fca_cia_aberta_{ano}.zip"
        self.datasets: dict[str, str] = self.find_dataset("fca_cia_aberta")

    def get_data(
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
        Obtém documentos referentes ao Formulário Cadastral (é um documento eletrônico,
        de encaminhamento periódico e eventual, previsto no artigo 22, inciso I, da Resolução CVM nº 80/22)

        ## Parâmetros

        - **dataset**: string contendo um dos valores em `self.datasets`.
        - **inicio**: inteiro referente ao ano inicial (inclusivo).
        - **fim**: inteiro referente ao ano final (exclusivo).
        """
        return super().get_data(dataset, inicio, fim)


class FRE(DataCVM):
    def __init__(self):
        self.url_dataset: str = "https://dados.cvm.gov.br/dataset/cia_aberta-doc-fre"
        self.url_base: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/"
        self.zip_template: str = "fre_cia_aberta_{ano}.zip"
        self.datasets: dict[str, str] = self.find_dataset("fre_cia_aberta")

    def get_data(
        self,
        dataset: Literal[
            "responsavel",
            "auditor",
            "auditor_responsavel",
            "informacao_financeira",
            "distribuicao_dividendos",
            "distribuicao_dividendos_classe_acao",
            "endividamento",
            "obrigacao",
            "historico_emissor",
            "grupo_economico_reestruturacao",
            "ativo_imobilizado",
            "ativo_intangivel",
            "participacao_sociedade",
            "participacao_sociedade_valorizacao_acao",
            "administrador_membro_conselho_fiscal",
            "membro_comite",
            "relacao_familiar",
            "relacao_subordinacao",
            "remuneracao_total_orgao",
            "remuneracao_maxima_minima_media",
            "posicao_acionaria",
            "posicao_acionaria_classe_acao",
            "distribuicao_capital",
            "distribuicao_capital_classe_acao",
            "transacao_parte_relacionada",
            "capital_social",
            "capital_social_classe_acao",
            "capital_social_titulo_conversivel",
            "capital_social_aumento",
            "capital_social_aumento_classe_acao",
            "capital_social_desdobramento",
            "capital_social_desdobramento_classe_acao",
            "capital_social_reducao",
            "capital_social_reducao_classe_acao",
            "direito_acao",
            "volume_valor_mobiliario",
            "outro_valor_mobiliario",
            "titular_valor_mobiliario",
            "mercado_estrangeiro",
            "titulo_exterior",
            "plano_recompra",
            "plano_recompra_classe_acao",
            "valor_mobiliario_tesouraria_movimentacao",
            "valor_mobiliario_tesouraria_ultimo_exercicio",
            "politica_negociacao",
            "politica_negociacao_cargo",
            "administrador_declaracao_genero",
            "administrador_declaracao_raca",
            "remuneracao_variavel",
            "remuneracao_acao",
            "acao_entregue",
            "empregado_posicao_declaracao_genero",
            "empregado_posicao_declaracao_raca",
            "empregado_posicao_faixa_etaria",
            "empregado_posicao_local",
            "empregado_local_declaracao_genero",
            "empregado_local_declaracao_raca",
            "empregado_local_faixa_etaria",
        ],
        inicio: int,
        fim: int,
    ) -> pd.DataFrame:
        """
        Obtém documentos referentes ao Formulário Cadastral (é um documento eletrônico,
        de encaminhamento periódico e eventual, previsto no artigo 22, inciso I, da Resolução CVM nº 80/22)

        ## Parâmetros

        - **dataset**: string contendo um dos valores em `self.datasets`.
        - **inicio**: inteiro referente ao ano inicial (inclusivo).
        - **fim**: inteiro referente ao ano final (exclusivo).
        """
        return super().get_data(dataset, inicio, fim)


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
