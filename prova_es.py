from elasticsearch import Elasticsearch

es = Elasticsearch("https://10.0.9.28:9200")

resp = es.search(index="jobs", body={"query": {"match_all": {}}})
for hit in resp["hits"]["hits"]:
    print(hit["_source"])
    
