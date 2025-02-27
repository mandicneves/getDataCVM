import requests
import zipfile
import pandas as pd
from io import BytesIO
from typing import Literal
from bs4 import BeautifulSoup


class DataCVM:
    def find_dataset(self, data_type: str) -> dict[str, str]:
        if "fca" in data_type:
            delimiter = "("
        elif "fre" in data_type:
            delimiter = ":"
        response = requests.get(self.url_dataset)
        html = BeautifulSoup(response.text, "html.parser")
        li_strong = [li for li in html.find_all("li") if li.find("strong")]
        dataset = dict()
        for li in li_strong:
            text = li.get_text(strip=True)
            if text.startswith(data_type.removesuffix("aberta")):
                upper_limit = text.find(delimiter)
                text = text[:upper_limit].replace("(anteriormente", "")
                if data_type in text:
                    key = text.removeprefix(data_type + "_")
                    value = f"{text}_" + "{year}.csv"
                else:
                    key = text.removeprefix(data_type.removesuffix("aberta"))
                    value = f"{data_type}_{key}_" + "{year}.csv"
                dataset[key] = value
        dataset["original"] = data_type + "_{year}.csv"
        return dataset

    def download_data(
        self, start: int, end: int, base_url: str, zip_template: str, csv_template: str
    ) -> pd.DataFrame:
        """
        Downloads data for the specified years using templates for the ZIP and CSV filenames.
        """
        data_list = []
        for year in range(start, end):
            zip_filename = zip_template.format(year=year)
            url = base_url + zip_filename

            try:
                r = requests.get(url, timeout=10)
                r.raise_for_status()

                with zipfile.ZipFile(BytesIO(r.content)) as zip_file:
                    csv_filename = csv_template.format(year=year)
                    with zip_file.open(csv_filename) as file:
                        lines = file.readlines()
                        lines = [line.strip().decode("ISO-8859-1") for line in lines]
                        lines = [line.split(";") for line in lines]
                        df = pd.DataFrame(data=lines[1:], columns=lines[0])
                        data_list.append(df)
                        print(f"Finished reading data for year {year}.")

            except requests.exceptions.RequestException as e:
                print(f"Error downloading data for year {year}: {e}")
            except zipfile.BadZipFile:
                print(f"Error unzipping file for year {year}.")
            except Exception as e:
                print(f"Unexpected error for year {year}: {e}")

        return pd.concat(data_list, ignore_index=True) if data_list else pd.DataFrame()

    def get_data(
        self,
        dataset: str,
        start: int,
        end: int,
    ) -> pd.DataFrame:
        """
        Generic method to retrieve data. Subclasses must define:
          - self.base_url
          - self.zip_template
          - self.datasets (a dictionary with the available dataset keys)

        Parameters:
          - dataset: name of the dataset to download.
          - start: starting year (inclusive).
          - end: ending year (exclusive).
        """
        if not hasattr(self, "datasets"):
            raise AttributeError("Attribute 'datasets' not defined in the class.")
        if dataset not in self.datasets:
            raise ValueError(
                f"Dataset '{dataset}' not found. Choose from: {list(self.datasets.keys())}"
            )
        csv_template = self.datasets[dataset]
        return self.download_data(
            start, end, self.base_url, self.zip_template, csv_template
        )


class RegData(DataCVM):
    def __init__(self):
        self.url = (
            "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"
        )

    def get_data(self) -> pd.DataFrame:
        """
        Registration data of publicly traded companies, such as CNPJ, registration date, and registration status.
        """
        r = requests.get(self.url)
        lines = r.text.split("\n")
        lines = [line.split(";") for line in lines]
        df = pd.DataFrame(data=lines[1:-1], columns=lines[0])

        return df


class FCA(DataCVM):
    def __init__(self):
        self.url_dataset: str = "https://dados.cvm.gov.br/dataset/cia_aberta-doc-fca"
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/"
        self.zip_template: str = "fca_cia_aberta_{year}.zip"
        self.datasets: dict[str, str] = self.find_dataset("fca_cia_aberta")

    def get_data(
        self,
        dataset: Literal[
            "original",
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
        start: int,
        end: int,
    ) -> pd.DataFrame:
        """
        Retrieves documents related to the Cadastral Form (an electronic document, since 2010,
        for periodic and occasional submission, as provided in Article 22, Item I, of CVM Resolution No. 80/22).

        ## Parameters

          1. **dataset:** a string containing one of the values in `self.datasets`.
          2. **start:** an integer representing the starting year (inclusive) - minimum year 2010.
          3. **end:** an integer representing the ending year (exclusive).
        """
        return super().get_data(dataset, start, end)


class FRE(DataCVM):
    def __init__(self):
        self.url_dataset: str = "https://dados.cvm.gov.br/dataset/cia_aberta-doc-fre"
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/"
        self.zip_template: str = "fre_cia_aberta_{year}.zip"
        self.datasets: dict[str, str] = self.find_dataset("fre_cia_aberta")

    def get_data(
        self,
        dataset: Literal[
            "original",
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
        start: int,
        end: int,
    ) -> pd.DataFrame:
        """
        Retrieves documents related to the FRE Form (an electronic document, since 2010,
        for periodic and occasional submission, as provided in Article 22, Item I, of CVM Resolution No. 80/22).

        ## Parameters

          1. **dataset:** a string containing one of the values in `self.datasets`.
          2. **start:** an integer representing the starting year (inclusive) - minimum year 2010.
          3. **end:** an integer representing the ending year (exclusive).
        """
        return super().get_data(dataset, start, end)


class IPE(DataCVM):
    def __init__(self):
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/"
        self.zip_template: str = "ipe_cia_aberta_{year}.zip"
        self.datasets: dict[str, str] = {"original": "ipe_cia_aberta_{year}.csv"}

    def get_data(
        self,
        start: int,
        end: int,
    ) -> pd.DataFrame:
        """
        The dataset provides unstructured documents from companies
        (periodic and occasional IPE filings) submitted in the last five years.

        ## Parameters

          1. **start:** an integer representing the starting year (inclusive) - minimum year: 2003.
          2. **end:** an integer representing the ending year (exclusive).
        """
        return super().get_data("original", start, end)


class ITR(DataCVM):
    def __init__(self):
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"
        self.zip_template: str = "itr_cia_aberta_{year}.zip"
        self.datasets: dict[str, str] = {
            "original": "itr_cia_aberta_{year}.csv",
            "bpa_con": "itr_cia_aberta_BPA_con_{year}.csv",
            "bpa_ind": "itr_cia_aberta_BPA_ind_{year}.csv",
            "bpp_con": "itr_cia_aberta_BPP_con_{year}.csv",
            "bpp_ind": "itr_cia_aberta_BPP_ind_{year}.csv",
            "dfc_md_con": "itr_cia_aberta_DFC_MD_con_{year}.csv",
            "dfc_md_ind": "itr_cia_aberta_DFC_MD_ind_{year}.csv",
            "dfc_mi_con": "itr_cia_aberta_DFC_MI_con_{year}.csv",
            "dfc_mi_ind": "itr_cia_aberta_DFC_MI_ind_{year}.csv",
            "dmpl_con": "itr_cia_aberta_DMPL_con_{year}.csv",
            "dmpl_ind": "itr_cia_aberta_DMPL_ind_{year}.csv",
            "dra_con": "itr_cia_aberta_DRA_con_{year}.csv",
            "dra_ind": "itr_cia_aberta_DRA_ind_{year}.csv",
            "dre_con": "itr_cia_aberta_DRE_con_{year}.csv",
            "dre_ind": "itr_cia_aberta_DRE_ind_{year}.csv",
            "dva_con": "itr_cia_aberta_DVA_con_{year}.csv",
            "dva_ind": "itr_cia_aberta_DVA_ind_{year}.csv",
            "parecer": "itr_cia_aberta_parecer_{year}.csv",
        }

    def get_data(
        self,
        dataset: Literal[
            "bpa_con",
            "bpa_ind",
            "bpp_con",
            "bpp_ind",
            "dfc_md_con",
            "dfc_md_ind",
            "dfc_mi_con",
            "dfc_mi_ind",
            "dmpl_con",
            "dmpl_ind",
            "dra_con",
            "dra_ind",
            "dre_con",
            "dre_ind",
            "dva_con",
            "dva_ind",
            "original",
            "parecer",
        ],
        start: int,
        end: int,
    ) -> pd.DataFrame:
        """
        The Quarterly Information Form (ITR) is an electronic document submitted periodically,
        as required by Article 22, item V, of CVM Resolution No. 80/22.

        ## Parameters

          1. **dataset:** a string containing one of the values in `self.datasets`.
          2. **start:** an integer representing the starting year (inclusive) - minimum year 2011.
          3. **end:** an integer representing the ending year (exclusive).
        """
        return super().get_data(dataset, start, end)


class VLMO(DataCVM):
    def __init__(self):
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/VLMO/DADOS/"
        self.zip_template: str = "vlmo_cia_aberta_{year}.zip"
        self.datasets: dict[str, str] = {
            "original": "vlmo_cia_aberta_{year}.csv",
            "consolidado": "vlmo_cia_aberta_con_{year}.csv",
        }

    def get_data(
        self,
        dataset: Literal["original", "consolidado"],
        start: int,
        end: int,
    ) -> pd.DataFrame:
        """
        Traded and Held Securities are periodically submitted information to the CVM,
        as required by Article 11 of CVM Resolution No. 44.

        ## Parameters

          1. **dataset:** a string containing one of the values in `self.datasets`.

          2. **start:** an integer representing the starting year (inclusive) - minimum year 2020.

          3. **end:** an integer representing the ending year (exclusive).
        """
        return super().get_data(dataset, start, end)


class ICBGC(DataCVM):
    def __init__(self):
        self.base_url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/CGVN/DADOS/"
        self.zip_template: str = "cgvn_cia_aberta_{year}.zip"
        self.datasets: dict[str, str] = {
            "original": "cgvn_cia_aberta_{year}.csv",
            "praticas": "cgvn_cia_aberta_praticas_{year}.csv",
        }

    def get_data(
        self,
        dataset: Literal["original", "praticas"],
        start: int,
        end: int,
    ) -> pd.DataFrame:
        """
        The Governance Code Report (ICBGC) is an electronic document
        submitted periodically, as required by Article 32 of CVM Resolution No. 80.

        ## Parameters

          1. **dataset:** a string containing one of the values in `self.datasets`.

          2. **start:** an integer representing the starting year (inclusive) - minimum year 2020.

          3. **end:** an integer representing the ending year (exclusive).
        """
        return super().get_data(dataset, start, end)


class DFP(DataCVM):
    def __init__(self):
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
        self.zip_template: str = "dfp_cia_aberta_{year}.zip"
        self.datasets: dict[str, str] = {
            "orgininal": "dfp_cia_aberta_{year}.csv",
            "bpa_con": "dfp_cia_aberta_BPA_con_{year}.csv",
            "bpa_ind": "dfp_cia_aberta_BPA_ind_{year}.csv",
            "bpp_con": "dfp_cia_aberta_BPP_con_{year}.csv",
            "bpp_ind": "dfp_cia_aberta_BPP_ind_{year}.csv",
            "dfc_md_con": "dfp_cia_aberta_DFC_MD_con_{year}.csv",
            "dfc_md_ind": "dfp_cia_aberta_DFC_MD_ind_{year}.csv",
            "dfc_mi_con": "dfp_cia_aberta_DFC_MI_con_{year}.csv",
            "dfc_mi_ind": "dfp_cia_aberta_DFC_MI_ind_{year}.csv",
            "dmpl_con": "dfp_cia_aberta_DMPL_con_{year}.csv",
            "dmpl_ind": "dfp_cia_aberta_DMPL_ind_{year}.csv",
            "dra_con": "dfp_cia_aberta_DRA_con_{year}.csv",
            "dra_ind": "dfp_cia_aberta_DRA_ind_{year}.csv",
            "dre_con": "dfp_cia_aberta_DRE_con_{year}.csv",
            "dre_ind": "dfp_cia_aberta_DRE_ind_{year}.csv",
            "dva_con": "dfp_cia_aberta_DVA_con_{year}.csv",
            "dva_ind": "dfp_cia_aberta_DVA_ind_{year}.csv",
            "parecer": "dfp_cia_aberta_parecer_{year}.csv",
        }

    def get_data(
        self,
        dataset: Literal[
            "bpa_con",
            "bpa_ind",
            "bpp_con",
            "bpp_ind",
            "dfc_md_con",
            "dfc_md_ind",
            "dfc_mi_con",
            "dfc_mi_ind",
            "dmpl_con",
            "dmpl_ind",
            "dra_con",
            "dra_ind",
            "dre_con",
            "dre_ind",
            "dva_con",
            "dva_ind",
            "orgininal",
            "parecer",
        ],
        start: int,
        end: int,
    ) -> pd.DataFrame:
        """
        The Standardized Financial Statements Form (DFP) is an electronic document submitted periodically,
        as required by Article 22, item IV, of CVM Resolution No. 80/22.

        ## Parameters

          1. **dataset:** a string containing one of the values in `self.datasets`.

          2. **start:** an integer representing the starting year (inclusive) - minimum year 2010.

          3. **end:** an integer representing the ending year (exclusive).
        """
        return super().get_data(dataset, start, end)
