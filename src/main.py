# %% 
import os
from urllib.parse import urlparse
import pandas as pd
import nltk
from nltk.tokenize import sent_tokenize

SCRAPE_JSON = "data/scrape/apify/newstead.json"
SCHOOLS_DATA = "data/schools"
print("Starting pipeline")


print("Reading scrape data folder")
scrapedDf = pd.read_json(SCRAPE_JSON, lines=True)
scrapedDf = scrapedDf[['url', 'text', 'fileUrl']]
scrapedDf['domain'] = scrapedDf['url'].apply(lambda x: urlparse(x).netloc)

# %% 
print("Populating directory for school")
groupedByDomain = scrapedDf.groupby('domain').size().reset_index(name='count')
topDomain = groupedByDomain.sort_values(by='count', ascending=False).head(1)['domain'].iloc[0]
print(f"Found domain to be likely: {topDomain}")
domainOnlyDf = scrapedDf[scrapedDf['domain'] == topDomain]
school_path = f"{SCHOOLS_DATA}/{topDomain}"
os.makedirs(school_path, exist_ok=True)

print("Filtering out entries with null text (e.g. pdfs) for now")
cleanedDf = domainOnlyDf[domainOnlyDf['text'].notna()]

print(f"Writing raw scrape file for school {topDomain}")
cleanedDf.to_json(f'{school_path}/scrape.json', 'records', lines=True)

print("Writing chunkified ")
# Ensure nltk punkt tokenizer is downloaded
nltk.download('punkt')

def chunk_text(text, max_chunk_length):
    sentences = sent_tokenize(text)
    chunks = []
    chunk = []
    current_length = 0

    for sentence in sentences:
        if current_length + len(sentence) > max_chunk_length:
            chunks.append(' '.join(chunk))
            chunk = []
            current_length = 0
        chunk.append(sentence)
        current_length += len(sentence)
    if chunk:
        chunks.append(' '.join(chunk))
        
    return chunks

# Variable
MAX_CHUNK_LENGTH = 200

chunks = []
for index, row in cleanedDf.iterrows():
    text_chunks = chunk_text(row['text'], MAX_CHUNK_LENGTH)
    for idx, chunk in enumerate(text_chunks):
        chunks.append({'url': row['url'], 'text': chunk, 'chunkindex': idx})

chunked_df = pd.DataFrame(chunks)
cleanedDf.to_json(f'{school_path}/chunked.json', 'records', lines=True)


# %%



# %%
