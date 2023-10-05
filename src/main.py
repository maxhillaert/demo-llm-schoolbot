# %%
from tasks.process_apify_scrape import process_apify_scrape
from tasks.chunking import create_simple_chunked_df
import pandas as pd


# %%
processed = process_apify_scrape("data/scrape/apify/newstead.json")
processed.to_csv("data/schools/www.newsteadwood.co.uk/scrape.json")

# %%
pd.read_csv("data/schools/www.newsteadwood.co.uk/scrape.json", )
chunked = create_simple_chunked_df(processed)
processed.to_csv("data/schools/www.newsteadwood.co.uk/chunked.json")
