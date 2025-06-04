## Chain.py

import json
import os
import mlflow
from operator import itemgetter
from databricks.vector_search.client import VectorSearchClient
from langchain.vectorstores import DatabricksVectorSearch
from databricks_langchain import DatabricksEmbeddings, ChatDatabricks
from langchain_core.runnables import RunnableLambda
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from typing import Optional, Dict
from mlflow.models.rag_signatures import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    StringResponse,
)
from langchain.output_parsers import ResponseSchema
from langchain.output_parsers import StructuredOutputParser
from dataclasses import dataclass, field
from langchain_core.prompts import (
    PromptTemplate,
    ChatPromptTemplate,
    MessagesPlaceholder,
)
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    ToolMessage,
    MessageLikeRepresentation,
)
from langchain_core.runnables import ConfigurableField
 
 ## Constants
catalog = "pdm-pdm-dl-quality-docs-genai-dev"
db = "gvault_test"

vs_endpoint_name = "vs_endpoint_gvault_test_sahil"
vs_index_fullname = f"{catalog}.{db}.vs_index_veeva_qdms_docs"

development_config = 'rag_chain_config.json'

llm_model_endpoint="pdm-dl-quality-docs-genai-anthropic-claude-3-5-sonnet-20241022"

embedding_model = DatabricksEmbeddings(endpoint="pdm-dl-quality-docs-genai-amazon-titan-embed-text-v2-0")

## Enable MLflow Tracing
mlflow.langchain.autolog()

#Get the conf from the local conf file
model_config = mlflow.models.ModelConfig(development_config=development_config)

retriever_config = model_config.get("retriever_config")

@dataclass
class CustomInputs():
    filters: Dict[str, str] = field(default_factory=lambda: {"doc_control_name": ['Kite RDMC', 'Kite TCF05', 'Kite HQ', 'Kite MVP01', 'Kite TCF03', 'Kite TCF04']})


# Additional input fields must be marked as Optional and have a default value
@dataclass
class CustomChatCompletionRequest(ChatCompletionRequest):
    custom_inputs: Optional[CustomInputs] = field(default_factory=CustomInputs)

def format_context(docs):
    chunk_contents = [
        {
            "chunk_text": d.page_content,
            "metadata": {
                "document_number": d.metadata["document_number"],
                "document_title": d.metadata["document_title"],
                "document_link": f'https://gvault.veevavault.com/ui/#doc_info/{int(d.metadata["document_id"])}/{int(d.metadata["document_major_version"])}/{int(d.metadata["document_minor_version"])}/',
                "document_page_number": int(d.metadata["document_page_number"]),
            }
        }
        for d in docs
    ]
    return json.dumps({"chunks": chunk_contents}, indent=4)

def create_configurable_with_filters(input: Dict) -> Dict:
    """
    create configurable object with filters.

    Args:
        input: The input data containing filters.

    Returns:
        A configurable object with filters added to the search_kwargs.
    """
    # if "custom_inputs" in input:
    #     filters = input["custom_inputs"]["filters"]
    # else:
    #     filters = {}
    filters={"doc_control_name": ['Kite RDMC', 'Kite TCF05', 'Kite HQ', 'Kite MVP01', 'Kite TCF03', 'Kite TCF04']}
    configurable = {
        "configurable": {
            "search_kwargs": {
                "k": 8,
                "filter": filters,
                "query_type": "HYBRID",
                "fetch_k": 40
            },
        }
    }
    return configurable

prompt = ChatPromptTemplate.from_messages(
    [
        (  # System prompt contains the instructions
            "system",
            """
            You are an AI assistant where your role is to provide accurate and concise answers to questions based strictly on the provided context information. If the question is unrelated to context information provided, politely decline to answer. If the answer is not found in the provided context, state that you don't know and avoid speculation.
            The provided context is in JSON format and contains document chunks with metadata. 

            Each chunk is formatted as follows:
            {{
                "chunk_text": "<chunk_text>",
                "metadata": {{
                    "document_number": "<document number>",
                    "document_title": "<document title>",
                    "document_link": "<URL to the document>",
                    "document_page_number": <page number>
                }}
            }}

            NOTE: <chunk_text> is in Mark down format

            If the answer is found in the context, It is very important to provide citation (details where the answer is derived from) in below format at the end of your response. DO NOT REPEAT SAME CITATION AGAIN in your answer. Add new line for each citation row and consolidate all different pages from each document_number and show it in the same row instead of multiple rows
            Citations:
            <Citation number>) [<document_number> (title: <document_title>, page(s): <document_page_number>)](<document_link>)

            Return the ouptput in a mark down format 

           Consider both the previous conversation (chat history) and the current question, If the latest question is not a follow up question or does not reference or rely on any context from the chat history then DO NOT use it while answering the question. Provide necessary context if referencing prior discussion.

            Previous conversation:
            {chat_history}

            Use the following context to answer the question:
            {context}

            """,
        ),
        # User's question
        ("user", "{question}"),
    ]
)

# Return the string contents of the most recent message from the user
def extract_user_query_string(chat_messages_array):
    return chat_messages_array[-1]["content"]

def extract_previous_messages(chat_messages_array):
    messages = "\n"
    for msg in chat_messages_array[:-1]:
        messages += (msg["role"] + ": " + msg["content"] + "\n")  
    return messages

def get_chat_history(chat_messages_array): 
    return chat_messages_array[:-1]

def combine_all_messages_for_vector_search(chat_messages_array):
    """
    Combines the current question with relevant context from chat history
    to create a rephrased question
    """
    system_prompt = (
    "Given a chat history and the latest user question "
    "which might reference context in the chat history, "
    "formulate a standalone question which can be understood "
    "without the chat history. Do NOT answer the question, "
    "just reformulate it if needed and otherwise return it as is."
    "DO NOT add The question should be reformulated as: and just return the result. i am using the result directly as input to another chain."
    "chat_history: {chat_history}"
    )

    rephrase_prompt = ChatPromptTemplate.from_messages([("system", system_prompt), ("human", "{input}")])

    chain_input={"input": extract_user_query_string(chat_messages_array),
     "chat_history": extract_previous_messages(chat_messages_array)}
    chain = rephrase_prompt | chat_model | StrOutputParser()

    result=chain.invoke(chain_input)
    # print("Rephrased question:", result)
    return result


def compare_revelancy_and_rewrite_query(chat_messages_array):
    """
    Combines the current question with relevant context from chat history
    to create a rephrased question
    """
    if len(chat_messages_array)<=1:
        # print("first ques. skipping llm call for rephrasing")
        return extract_user_query_string(chat_messages_array)

    system_prompt = """
    You are an expert AI assistant proficient in rephrasing a self contained question.
    You have access to chat history and your task is to understand the latest user question and rephrase it into a self contained question using the previous context if and only if it is relevant to the chat history. 
    If the latest user question is not a follow up question to previous user questions or does not reference or rely on any context from the chat history, return the latest user question without rephrasing or elaborating or expanding anything and most importantly DO NOT speculate or add words into the question.

    Format the output as JSON with the following keys:
    question

    Chat History:
    {chat_history}
    """

    rephrase_prompt = ChatPromptTemplate.from_messages([("system", system_prompt), ("user", "{latest_question}")])

    question_schema = ResponseSchema(
    name="question",
    description="rephrased question or as is latest question"
    )

    question_response_schemas = [
        question_schema,
    ]

    rephrased_question_output_parser = StructuredOutputParser.from_response_schemas(question_response_schemas)

    chain_input={"latest_question": extract_user_query_string(chat_messages_array),
     "chat_history": get_chat_history(chat_messages_array)}
    # print(f"rephrase_prompt: {rephrase_prompt}")
    chain = rephrase_prompt | chat_model | rephrased_question_output_parser

    result=chain.invoke(chain_input)
    # print("Rephrased question:", result)
    if (type(result) != dict) or ("question" not in result):
        print(f"Invalid result: {result}")
        return extract_user_query_string(chat_messages_array)
    
    return result["question"]


def safe_vector_search(input_data):
    """
    Perform vector search with metadata filtering and conversation context
    """
    # Combine current question with chat history for context-aware search
    query = compare_revelancy_and_rewrite_query(input_data["messages"])
    config = create_configurable_with_filters(input_data)
    
    return configurable_vs_retriever.invoke(
        input=query,
        config=config
    )



# Connect to the Vector Search Index
vs_client = VectorSearchClient(disable_notice=True)

vs_index = vs_client.get_index(
    endpoint_name=vs_endpoint_name,
    index_name=vs_index_fullname
)
vector_search_schema = retriever_config.get("schema")


vector_search_as_retriever = DatabricksVectorSearch(
        vs_index, text_column="chunk_text", embedding=embedding_model, columns=["document_title", "document_name", "document_number", 'document_id', 'document_major_version', 'document_minor_version', 'document_page_number']
    ).as_retriever()

# "fetch_k": 40 search_type="mmr"
configurable_vs_retriever = vector_search_as_retriever.configurable_fields(
    search_kwargs=ConfigurableField(
        id="search_kwargs",
        name="Search Kwargs",
        description="The search kwargs to use",
    )
)

mlflow.models.set_retriever_schema(
    primary_key=vector_search_schema.get("chunk_id"),
    text_column=vector_search_schema.get("chunk_text"),
    doc_uri=vector_search_schema.get("document_number"),
    
)

# def format_context(docs):
#     chunk_template = retriever_config.get("chunk_template")
#     chunk_contents = [
#         chunk_template.format(
#             chunk_text=d.page_content,
#         )
#         for d in docs
#     ]
#     return "".join(chunk_contents)

# prompt = PromptTemplate(
#     template=llm_config.get("llm_prompt_template"),
#     input_variables=llm_config.get("llm_prompt_template_variables"),
# )

chat_model = ChatDatabricks(endpoint=llm_model_endpoint, max_tokens = 200000, temperature=0)   

# chain = (
#     {
#         "question": itemgetter("messages") | RunnableLambda(extract_user_query_string),
#         "context": itemgetter("messages")
#         | RunnableLambda(combine_all_messages_for_vector_search)
#         | vector_search_as_retriever
#         | RunnableLambda(format_context),
#         "chat_history": itemgetter("messages") | RunnableLambda(extract_previous_messages)
#     }
#     | prompt
#     | model
#     | StrOutputParser()
# )

# def safe_invoke_retriever(input):
#     # print(f"input: {input}")         
#     result = configurable_vs_retriever.invoke(input=extract_user_query_string(input["messages"]), config=create_configurable_with_filters(input))
#     if not result or len(result) == 0:
#         return {"page_content": "", "metadata": {}}
#     return result

chain = (
    {
        "question": itemgetter("messages") | RunnableLambda(extract_user_query_string),
        "context": RunnablePassthrough() 
        | RunnableLambda(safe_vector_search)
        | RunnableLambda(format_context),
        "chat_history": itemgetter("messages") | RunnableLambda(get_chat_history)
    }
    | prompt
    | chat_model
    | StrOutputParser()
)

# Tell MLflow logging where to find your chain.
mlflow.models.set_model(model=chain)