from langchain.chains import RetrievalQA
from langchain.vectorstores.chroma import Chroma
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.llms.openai import OpenAI
from langchain.retrievers.self_query.base import SelfQueryRetriever
from langchain.chains.query_constructor.base import AttributeInfo
import logging


def printdocs(resp):
    print(f"Found {len(resp)} docs")

    i = 0
    for r in resp:
        d = r
        print("-------")
        print(f"Document {i}:")
        print("-------")
        print("Metadata:")
        print(f"Domain: {d.metadata['domain']}")
        print(f"Title: {d.metadata['title']}")
        print(f"URL: {d.metadata['url']}")
        print("")
        print(d.page_content)
        print("")
        i = i + 1


embedding = OpenAIEmbeddings()
db = Chroma(persist_directory="data/embeddings/recursive",
            embedding_function=embedding)

resp = db.max_marginal_relevance_search(
    "When is the 11 plus exam?", 10, fetch_k=100)

# printdocs(resp)

document_content_description = "School Website Pages"
llm = OpenAI(temperature=0, model="gpt-3.5-turbo-instruct")
metadata_field_info = [
    AttributeInfo(
        name="domain",
        description="the domain name of the school website the chunk belongs to.",
        type="string",
    ),
    AttributeInfo(
        name="title",
        description="The title of the web page of the school website the chunk belongs to which may contain contextual information",
        type="string",
    ),
    AttributeInfo(
        name="url",
        description="The url of the web page of the school website the chunk belongs to",
        type="string",
    ),
]


retriever = db.as_retriever(kwargs={"k": 100, "fetch_k": 500})

logging.basicConfig(level=logging.CRITICAL)


question = "What are the year 7 admissions criteria and when are the exams?"
# resp = retriever.get_relevant_documents(question)
# printdocs(resp)

qa_chain = RetrievalQA.from_chain_type(
    llm,
    retriever=db.as_retriever(),
    chain_type="map_reduce"
)

result = qa_chain({"query": question})
print(result['result'])
