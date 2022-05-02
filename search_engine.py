#!/usr/bin/env python3
from elasticsearch import Elasticsearch

data_structure = {
  "settings": {
    "index": {
      "number_of_shards": 2,
      "number_of_replicas": 2
    },
    "analysis": {
      "analyzer": "standard"
    }
  },
    "mappings": {
        "properties": { 
            "seq_id": { "type": "integer" },
            "medline_ui": { "type": "integer" },
            "source": { "type": "text" },
            "mesh_terms": { "type": "text" },
            "title": { "type": "text" },
            "publication_type": { "type": "text" },
            "abstract": { "type": "text" },
            "authors": { "type": "text" }
        }
    }
}
stop_words = ["a", "an", "and", "are", "as", "at", "are", 
    "be", "but", "by", "for", "if", "in", "into", "is", "it", 
    "no", "not", "of", "on", "or", "such", "that", "the", "their", 
    "then", "there", "these", "they", "this", "to", "was", "will", "with", "when"]

def create_index(client, index):
    resp = client.create(index=index,id="1", document=data_structure)

data_tags = {".I": "seq_id", ".U": "medline_ui", ".S": "source", ".M": "mesh_terms", 
             ".T": "title", ".P": "publication_type", ".W": "abstract", ".A": "authors"}

def read_blocks(file):
    block = ''
    for line in file:
        if line.startswith(".I") and len(block) > 0:
            yield block
            block = ''
        block += line
    yield block

def read_query_blocks(file):
    block = ''
    for line in file:
        if line.startswith("<top>") and len(block) > 0:
            yield block
            block = ''
        block += line
    yield block

def index_documents(file):
    with open(file, "r") as f:
        for block in read_blocks(f):
            lines = block.splitlines()
            local_dict = {}
            for i in range(len(lines)):
                line = lines[i]
                desc = line.strip().split(' ',1)
                tag = desc[0]
                if tag in data_tags.keys():
                    key = data_tags.get(tag)
                    if key == "seq_id":
                        local_dict[key] = int(desc[1])
                    elif key == "medline_ui":
                        local_dict[key] = int(lines[i + 1].strip())
                    elif key == "authors" or key == "mesh_terms":
                        local_dict[key] = lines[i + 1].strip('.').split("; ")
                    else:
                        local_dict[key] = lines[i + 1].strip()
            resp = client.index(index="med_documents_v1", id=local_dict["medline_ui"], document=local_dict)

def tokenize(line):
    content = []
    words = line.split(" ")
    for word in words:
        if word not in stop_words:
            content.append(word)
    return content

def create_query_list(file):
    queries = []
    with open(file, "r") as f:
        for block in read_query_blocks(f):
            local_dict = {}
            lines = block.splitlines()
            for i in range(len(lines)):
                line = lines[i]
                if line.startswith("<num>"):
                    number = line.strip("<num> ").split(':')
                    local_dict["num"] = number[1].strip()
                elif line.startswith("<title>"):
                    title = line.strip("<title> ")
                    local_dict["title"] =  title 
                elif line.startswith("<desc>"):
                    line = lines[i + 1]
                    local_dict["desc"] = line
            queries.append(local_dict)
    return queries

def get_document_call(client):
    response = client.get(index="med_documents_v1", id=54725)
    # print(response)

def create_out_files(result, file, query, type):
    for i in range(len(result['hits']['hits'])):
        hit = result['hits']['hits'][i]
        query_id = query['num']
        doc_id = hit['_id']
        rank = i
        score = hit['_score']
        file.write(str(query_id) + "\t" + "Q0" + "\t" + str(doc_id) + "\t" + str(rank) + "\t" + str(score) +  "\t" +  type + "\n")

def search_bool_call(client, qry, file, index):
    body = {
        "query": {
            "bool": {
                "must": {"match": { "title": { "query": qry['title'] }}},
                "should": {"match": { "abstract": {"boost": 2, "query": qry['desc'] }}}
            }
        }
    }
    result = client.search(index = index, track_scores=True, size=50, body = body)
    create_out_files(result, file, qry, "bool")

def search_tf(client, qry, file, index):
    body = {
        "query": {
            "match_phrase": {
                "title": qry['title'], "boost": 2
            },
            "match_phrase": {
                "title": qry['desc']
            }
        }
    }
    result = client.search(index = index, track_scores=True, size=50, body = body)
    create_out_files(result, file, qry, "tf")

def search_tf_idf(client, qry, file, index):
    body = {
        "query": {
            "match":{ "title": {  "query": qry['title']} },
            "match":{ "abstract": {"boost": 2, "query": qry['desc']}}
        }
    }
    result = client.search(index = index, size=50, body = body)
    create_out_files(result, file, qry, "tf-idf")

def parse_file(query):
    ratings = []
    indexes = []

    with open("qrels.ohsu.88-91", 'r') as f:
        lines = f.readlines()
        for line in lines:
            words = line.split()
            if words[0] == query['num']:
                ratings.append(words[3])
                indexes.append(words[2])
    return ratings, indexes


def serach_relevance_feedback(client, qry, file, index):
    ratings, indexes = parse_file(qry)
    req, metric = rank_eval_call(index, qry['num'], qry['title'], ratings[:20], indexes[:20])
    rank_result = client.rank_eval(index = index, requests = req, metric = metric)
    hits = rank_result["details"][qry['num']]["hits"]
    
    with open(file, 'w') as f:
        for i in range(len(hits)):
            file_line = qry['num'] + "\t"+ "Q0" + "\t"  + str(hits[i]["hit"]["_id"])+ "\t" + str(i) + "\t" + str(hits[i]["hit"]["_score"]) + "\t" + "relevence" + "\n"
            f.write(file_line)

def rank_eval_call(index, q_id, title, ratings, indexes):
    request = [{
        "id": q_id,
        "request": {
            "query": {
                "multi_match":{
                    "query": title,
                    "fields": ["title", "abstract"]
                }
            }
        },
        "ratings": [{
            "_index": index, "_id": indexes[i], "rating": ratings[i] }for i in range(len(ratings))
        ]
    }]
    metric = {
        "mean_reciprocal_rank": {
            "k": 20,
            "relevant_rating_threshold": 0
        }
    }
    return request, metric

def search_custom(client, qry, file, index):
    body = {
        "query":{
            "function_score": {
                "query": {
                    "match":{ "title": { "query": qry['title']} },
                    "match":{ "abstract": { "query": qry['desc']}}
                    },
                    "boost": "5",
                "functions": [{
                    "filter": { "match": { "title": { "query": qry['title']} } },
                    "random_score": {}, 
                    "weight": 20
                    },
                    {
                        "filter": { "match": { "abstract":  { "query": qry['desc']} } },
                        "weight": 40
                        }],
                "max_boost": 42,
                "score_mode": "multiply",
                "boost_mode": "multiply",
            }
        }
    }
    result = client.search(index = index, size=50, body = body)
    create_out_files(result, file, qry, "custom")

if __name__ == "__main__":
    # Password for the 'elastic' user generated by Elasticsearch
    ELASTIC_PASSWORD = "v42-Jj3mUx8pXT1jN=lW"
    index = "med_documents_v1"
    # index = "med_documents_v1"

    # Create the client instance
    client = Elasticsearch(
        "https://localhost:9200",
        ca_certs="/Users/smruthipobbathi/elasticsearch-8.1.3/config/certs/http_ca.crt",
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )
    # create_index(client, index)
    # index_documents("ohsumed.88-91")
    queries = create_query_list("query.ohsu.1-63")
    get_document_call(client)
    bool_file = open('boolean_retrieval.txt','w')
    tf_file = open('tf.txt', 'w')
    tf_idf_file = open('tf-idf.txt', 'w')
    custom_file = open('custom.txt', 'w')

    for query in queries:
        search_bool_call(client, query, bool_file, index) 
        search_tf(client, query, tf_file, index)
        search_tf_idf(client, query, tf_idf_file, index)
        serach_relevance_feedback(client, query, "relevance_file", index)
        search_custom(client, query, custom_file, index)
        
