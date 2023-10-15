# %%
from tasks.process_apify_scrape import process_apify_scrape

# %%
school_dir = "data/schools/www.newsteadwood.co.uk"
processed = process_apify_scrape("data/scrape/apify/newstead.json", school_dir)
processed.to_json(
    f"{school_dir}/scrape.json", orient="records", lines=True)


school_dir = "data/schools/www.breaside.co.uk"
processed = process_apify_scrape("data/scrape/apify/breaside.json", school_dir)
processed.to_json(
    f"{school_dir}/scrape.json", orient="records", lines=True)
