import pandas as pd
from urllib.parse import urlparse, unquote
import os
import requests

import logging

logger = logging.getLogger(__name__)

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
    dataframe = pd.read_json(
        json_path, orient='records', lines=True)

    # Assuming 'metadata' is the column with the nested dictionaries and you want to extract the 'title'
    dataframe['title'] = dataframe['metadata'].apply(
        lambda x: x.get('title', None))

    dataframe = dataframe[['url', 'text', 'fileUrl', 'title']]
    return dataframe


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


def where_text_null(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out rows with null text from a pandas DataFrame.

    Args:
        dataframe (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: The filtered DataFrame.
    """
    return dataframe[dataframe['text'].notna()]


def download_file(url: str, dir_name: str) -> str:
    # Get the file name from the URL
    file_name = unquote(urlparse(url).path.split("/")[-1])
    local_path = os.path.join(dir_name, file_name)

    # Ensure directory exists
    os.makedirs(dir_name, exist_ok=True)

    try:
        response = requests.get(url, stream=True)
        # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        response.raise_for_status()

        # Download the file and save it locally
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return local_path
    except requests.RequestException as e:
        logger.warning(f"Failed to download {url}. Reason: {e}")
        return None


def process_file_urls(row: pd.Series, download_files_path: str):
    file_url = row['fileUrl']
    if pd.notnull(file_url):
        ext = file_url.split('.')[-1]
        local_path = download_file(
            file_url, os.path.join(download_files_path, ext))
        return local_path
    return None


def process_apify_scrape(input_json_path: str, download_files_path: str) -> pd.DataFrame:
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
    print("Stripping title whitespace")
    df['title'] = df['title'].str.strip()
    print(f"Filtered data to {len(df)} rows with domain {top_domain}")
    print("Downloading files...")
    df['fileUrlLocal'] = df.apply(
        lambda r: process_file_urls(r, download_files_path),
        axis=1
    )
    return df
