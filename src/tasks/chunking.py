import pandas as pd
import nltk
from nltk.tokenize import sent_tokenize

MAX_CHUNK_LENGTH = 200


def chunk_text(text: str) -> str:
    sentences = sent_tokenize(text)
    chunks = []
    chunk = []
    current_length = 0
    for sentence in sentences:
        if current_length + len(sentence) > MAX_CHUNK_LENGTH:
            chunks.append(' '.join(chunk))
            chunk = []
            current_length = 0
        chunk.append(sentence)
        current_length += len(sentence)
    if chunk:
        chunks.append(' '.join(chunk))
    return chunks


def create_chunks_df(df: pd.DataFrame) -> pd.DataFrame:
    chunks = []
    for _, row in df.iterrows():
        text_chunks = chunk_text(row['text'])
        for idx, chunk in enumerate(text_chunks):
            chunks.append(
                {'url': row['url'], 'text': chunk, 'chunkindex': idx})
    return pd.DataFrame(chunks)


def create_simple_chunked_df(df: pd.DataFrame) -> pd.DataFrame:
    print("Writing chunkified data")
    nltk.download('punkt')
    chunked_df = create_chunks_df(df)
    return chunked_df
