# %%
from tasks.process_apify_scrape import process_apify_scrape
from tasks.chunking import create_simple_chunked_df
import pandas as pd
from langchain.document_loaders.dataframe import DataFrameLoader

# %%
processed = process_apify_scrape("data/scrape/apify/newstead.json")
processed.to_json(
    "data/schools/www.newsteadwood.co.uk/scrape.json", orient="records", lines=True)

# # %%
# pd.read_csv("data/schools/www.newsteadwood.co.uk/scrape.json", )
# chunked = create_simple_chunked_df(processed)
# processed.to_csv("data/schools/www.newsteadwood.co.uk/chunked.json")

# %%
loader = DataFrameLoader(processed, page_content_column="text")
docs = loader.load()
