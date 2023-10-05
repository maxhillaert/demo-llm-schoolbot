import pandas as pd
import os
from urllib.parse import urlparse


SCHOOLS_DATA: str = "data/schools"


def read_data(json_path: str) -> pd.DataFrame:
    """
    Reads in data from a JSON file.

    Args:
        json_path (str): The path to the input JSON file.

    Returns:
        pandas.DataFrame: The data as a pandas DataFrame.
    """
    print("Reading scrape data")
    dataframe = pd.read_json(json_path, lines=True)
    return dataframe[['url', 'text', 'fileUrl']]


def extract_domain(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts the domain from a DataFrame containing URLs.

    Args:
        dataframe (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: The DataFrame with an added 'domain' column.
    """
    dataframe['domain'] = dataframe['url'].apply(lambda x: urlparse(x).netloc)
    return dataframe


def get_top_domain(dataframe: pd.DataFrame) -> str:
    """
    Gets the top domain from a DataFrame.

    Args:
        dataframe (pandas.DataFrame): The input DataFrame.

    Returns:
        str: The top domain.
    """
    grouped = dataframe.groupby('domain').size().reset_index(name='count')
    top_domain = grouped.sort_values(by='count', ascending=False).head(1)[
        'domain'].iloc[0]
    return top_domain


def where_domain_name(dataframe: pd.DataFrame, domain: str) -> pd.DataFrame:
    """
    Filters a DataFrame by domain.

    Args:
        dataframe (pandas.DataFrame): The input DataFrame.
        domain (str): The domain to filter by.

    Returns:
        pandas.DataFrame: The filtered DataFrame.
    """
    return dataframe[dataframe['domain'] == domain]


def create_directory(domain: str) -> str:
    """
    Creates a directory for a given domain.

    Args:
        domain (str): The domain name.

    Returns:
        str: The path to the created directory.
    """
    school_path = f"{SCHOOLS_DATA}/{domain}"
    os.makedirs(school_path, exist_ok=True)
    return school_path


def where_text_null(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out rows with null text from a pandas DataFrame.

    Args:
        dataframe (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: The filtered DataFrame.
    """
    return dataframe[dataframe['text'].notna()]


def process_apify_scrape(input_json_path: str) -> pd.DataFrame:
    """
    Processes an Apify scrape by reading in the data from a JSON file, extracting the domain,
    filtering the data by the top domain, and filtering out entries with null text.

    Args:
        input_json_path (str): The path to the input JSON file.

    Returns:
        pandas.DataFrame: The processed data as a pandas DataFrame.
    """

    print(f"Processing apify scrape at path {input_json_path}")
    df = read_data(input_json_path)
    print(f"Read in {len(df)} rows of data")
    df = extract_domain(df)
    print(f"Extracted domain from {len(df)} rows of data")
    top_domain = get_top_domain(df)
    print(f"Top domain is {top_domain}")
    df = where_domain_name(df, top_domain)
    print(f"Filtered data to {len(df)} rows with domain {top_domain}")
    df = where_text_null(df)
    print(f"Filtered data to {len(df)} rows with non-null text")
    return df

    return df
