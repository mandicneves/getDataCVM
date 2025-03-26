"""
getDataCVM.py

This module provides classes to download and process data from CVM.
Classes included:
    - DataCVM: Base class with generic methods.
    - RegData: For registration data.
    - FCA: For FCA data.
    - FRE: For FRE data.
    - IPE: For IPE data.
    - ITR: For ITR data.
    - VLMO: For VLMO data.
    - ICBGC: For ICBGC data.
    - DFP: For DFP data.
"""

import requests
import zipfile
import pandas as pd
from io import BytesIO
from typing import Dict, Any, List, Literal
from bs4 import BeautifulSoup


class DataCVM:
    def find_dataset(self, data_type: str) -> Dict[str, str]:
        """
        Retrieves a dictionary of dataset keys and corresponding CSV filename templates from the CVM dataset page.

        The method performs an HTTP GET request to self.url_dataset, parses the HTML to find list items
        containing dataset information, and builds a dictionary mapping a dataset key to a CSV filename template.
        An additional entry with key "original" is added.

        Parameters:
            data_type (str): The dataset type identifier (e.g., "fca_cia_aberta", "fre_cia_aberta").

        Returns:
            Dict[str, str]: A dictionary where each key is a dataset identifier (str) and each value is a CSV filename template.
        """
        if "fca" in data_type:
            delimiter: str = "("
        elif "fre" in data_type:
            delimiter = ":"
        else:
            delimiter = ":"  # valor padrão, se necessário

        response: requests.Response = requests.get(self.url_dataset)
        html: BeautifulSoup = BeautifulSoup(response.text, "html.parser")
        li_strong: List[Any] = [li for li in html.find_all("li") if li.find("strong")]
        dataset: Dict[str, str] = {}

        for li in li_strong:
            text: str = li.get_text(strip=True)
            if text.startswith(data_type.removesuffix("aberta")):
                upper_limit: int = text.find(delimiter)
                text = text[:upper_limit].replace("(anteriormente", "")
                if data_type in text:
                    key: str = text.removeprefix(data_type + "_")
                    value: str = f"{text}_" + "{year}.csv"
                else:
                    key = text.removeprefix(data_type.removesuffix("aberta"))
                    value = f"{data_type}_{key}_" + "{year}.csv"
                dataset[key] = value

        # Adiciona uma entrada padrão "original"
        dataset["original"] = data_type + "_{year}.csv"
        return dataset

    def download_data(
        self,
        start: int,
        end: int,
        base_url: str,
        zip_template: str,
        csv_template: str,
    ) -> pd.DataFrame:
        """
        Downloads and concatenates data from CVM for a range of years using ZIP and CSV filename templates.

        For each year in the range [start, end), the method constructs the ZIP filename using the zip_template,
        downloads the ZIP file from base_url, extracts the CSV file specified by csv_template,
        reads its content into a pandas DataFrame, and appends it to a list. Finally, all DataFrames are concatenated.

        Parameters:
            start (int): The starting year (inclusive).
            end (int): The ending year (exclusive).
            base_url (str): The base URL from which the ZIP files are downloaded.
            zip_template (str): A string template for the ZIP filename (e.g., "fca_cia_aberta_{year}.zip").
            csv_template (str): A string template for the CSV filename inside the ZIP (e.g., "fca_cia_aberta_geral_{year}.csv").

        Returns:
            pd.DataFrame: A DataFrame containing the concatenated data from all processed years.
        """
        data_list: List[pd.DataFrame] = []
        for year in range(start, end):
            zip_filename: str = zip_template.format(year=year)
            url: str = base_url + zip_filename

            try:
                r: requests.Response = requests.get(url, timeout=10)
                r.raise_for_status()

                with zipfile.ZipFile(BytesIO(r.content)) as zip_file:
                    csv_filename: str = csv_template.format(year=year)
                    with zip_file.open(csv_filename) as file:
                        lines: List[bytes] = file.readlines()
                        decoded_lines: List[str] = [
                            line.strip().decode("ISO-8859-1") for line in lines
                        ]
                        split_lines: List[List[str]] = [
                            line.split(";") for line in decoded_lines
                        ]
                        df: pd.DataFrame = pd.DataFrame(
                            data=split_lines[1:], columns=split_lines[0]
                        )
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
        Generic method to retrieve data for a specified dataset over a range of years.

        Subclasses must define the following attributes:
            - self.base_url: The base URL for downloading data.
            - self.zip_template: The ZIP filename template.
            - self.datasets: A dictionary mapping dataset keys to CSV filename templates.

        Parameters:
            dataset (str): The key of the dataset to download.
            start (int): The starting year (inclusive).
            end (int): The ending year (exclusive).

        Returns:
            pd.DataFrame: A DataFrame containing the concatenated data.

        Raises:
            AttributeError: If the subclass does not define the 'datasets' attribute.
            ValueError: If the provided dataset key is not found in self.datasets.
        """
        if not hasattr(self, "datasets"):
            raise AttributeError("Attribute 'datasets' not defined in the class.")
        if dataset not in self.datasets:
            raise ValueError(
                f"Dataset '{dataset}' not found. Choose from: {list(self.datasets.keys())}"
            )
        csv_template: str = self.datasets[dataset]
        return self.download_data(
            start, end, self.base_url, self.zip_template, csv_template
        )


class RegData(DataCVM):
    def __init__(self) -> None:
        """
        Initializes the RegData class with a URL for registration data.
        """
        self.url: str = (
            "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"
        )

    def get_data(self) -> pd.DataFrame:
        """
        Retrieves registration data for publicly traded companies.

        The data includes information such as CNPJ, registration date, and registration status.

        Returns:
            pd.DataFrame: A DataFrame containing the registration data.
        """
        r: requests.Response = requests.get(self.url)
        lines: List[str] = r.text.split("\n")
        split_lines: List[List[str]] = [line.split(";") for line in lines]
        df: pd.DataFrame = pd.DataFrame(data=split_lines[1:-1], columns=split_lines[0])
        return df


class FCA(DataCVM):
    def __init__(self) -> None:
        """
        Initializes the FCA class with URLs and filename templates specific to FCA data.
        """
        self.url_dataset: str = "https://dados.cvm.gov.br/dataset/cia_aberta-doc-fca"
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/"
        self.zip_template: str = "fca_cia_aberta_{year}.zip"
        self.datasets: Dict[str, str] = self.find_dataset("fca_cia_aberta")

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
        Retrieves FCA data for the specified dataset and year range.

        Parameters:
            dataset (Literal[...]): A string containing one of the allowed dataset keys.
            start (int): The starting year (inclusive) – minimum year 2010.
            end (int): The ending year (exclusive).

        Returns:
            pd.DataFrame: A DataFrame containing the concatenated FCA data.
        """
        return super().get_data(dataset, start, end)


class FRE(DataCVM):
    def __init__(self) -> None:
        """
        Initializes the FRE class with URLs and filename templates specific to FRE data.
        """
        self.url_dataset: str = "https://dados.cvm.gov.br/dataset/cia_aberta-doc-fre"
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/"
        self.zip_template: str = "fre_cia_aberta_{year}.zip"
        self.datasets: Dict[str, str] = self.find_dataset("fre_cia_aberta")

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
        Retrieves FRE data for the specified dataset and year range.

        Parameters:
            dataset (Literal[...]): A string containing one of the allowed dataset keys.
            start (int): The starting year (inclusive) – minimum year 2010.
            end (int): The ending year (exclusive).

        Returns:
            pd.DataFrame: A DataFrame containing the concatenated FRE data.
        """
        return super().get_data(dataset, start, end)


class IPE(DataCVM):
    def __init__(self) -> None:
        """
        Initializes the IPE class with URLs and filename templates specific to IPE data.
        """
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/"
        self.zip_template: str = "ipe_cia_aberta_{year}.zip"
        self.datasets: Dict[str, str] = {"original": "ipe_cia_aberta_{year}.csv"}

    def get_data(
        self,
        start: int,
        end: int,
    ) -> pd.DataFrame:
        """
        Retrieves IPE data (unstructured company filings) for the specified year range.

        Parameters:
            start (int): The starting year (inclusive) – minimum year 2003.
            end (int): The ending year (exclusive).

        Returns:
            pd.DataFrame: A DataFrame containing the concatenated IPE data.
        """
        return super().get_data("original", start, end)


class ITR(DataCVM):
    def __init__(self) -> None:
        """
        Initializes the ITR class with URLs and filename templates specific to ITR data.
        """
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"
        self.zip_template: str = "itr_cia_aberta_{year}.zip"
        self.datasets: Dict[str, str] = {
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
        Retrieves ITR data for the specified dataset and year range.

        Parameters:
            dataset (Literal[...]): A string containing one of the allowed dataset keys.
            start (int): The starting year (inclusive) – minimum year 2011.
            end (int): The ending year (exclusive).

        Returns:
            pd.DataFrame: A DataFrame containing the concatenated ITR data.
        """
        return super().get_data(dataset, start, end)


class VLMO(DataCVM):
    def __init__(self) -> None:
        """
        Initializes the VLMO class with URLs and filename templates specific to VLMO data.
        """
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/VLMO/DADOS/"
        self.zip_template: str = "vlmo_cia_aberta_{year}.zip"
        self.datasets: Dict[str, str] = {
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
        Retrieves VLMO data for the specified dataset and year range.

        Parameters:
            dataset (Literal["original", "consolidado"]): A string containing one of the allowed dataset keys.
            start (int): The starting year (inclusive) – minimum year 2020.
            end (int): The ending year (exclusive).

        Returns:
            pd.DataFrame: A DataFrame containing the concatenated VLMO data.
        """
        return super().get_data(dataset, start, end)


class ICBGC(DataCVM):
    def __init__(self) -> None:
        """
        Initializes the ICBGC class with URLs and filename templates specific to ICBGC data.
        """
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/CGVN/DADOS/"
        self.zip_template: str = "cgvn_cia_aberta_{year}.zip"
        self.datasets: Dict[str, str] = {
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
        Retrieves ICBGC data for the specified dataset and year range.

        Parameters:
            dataset (Literal["original", "praticas"]): A string containing one of the allowed dataset keys.
            start (int): The starting year (inclusive) – minimum year 2020.
            end (int): The ending year (exclusive).

        Returns:
            pd.DataFrame: A DataFrame containing the concatenated ICBGC data.
        """
        return super().get_data(dataset, start, end)


class DFP(DataCVM):
    def __init__(self) -> None:
        """
        Initializes the DFP class with URLs and filename templates specific to DFP data.
        """
        self.base_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
        self.zip_template: str = "dfp_cia_aberta_{year}.zip"
        self.datasets: Dict[str, str] = {
            "original": "dfp_cia_aberta_{year}.csv",
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
            "original",
            "parecer",
        ],
        start: int,
        end: int,
    ) -> pd.DataFrame:
        """
        Retrieves DFP data for the specified dataset and year range.

        Parameters:
            dataset (Literal[...]): A string containing one of the allowed dataset keys.
            start (int): The starting year (inclusive) – minimum year 2010.
            end (int): The ending year (exclusive).

        Returns:
            pd.DataFrame: A DataFrame containing the concatenated DFP data.
        """
        return super().get_data(dataset, start, end)
