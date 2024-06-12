from elasticsearch import Elasticsearch

# Connect to Elasticsearch instance
es = Elasticsearch("https://10.0.9.28:9200")

# Search for all documents in the "jobs" index
resp = es.search(index="jobs", body={"query": {"match_all": {}}})

# Iterate through search results and print document sources
for hit in resp["hits"]["hits"]:
    print(hit["_source"])
