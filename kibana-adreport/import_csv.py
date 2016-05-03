"""
Import csv data and create index to elasticsearch.
*This code is prototype*
"""

import pandas as pd
from elasticsearch import Elasticsearch

INDEX_NAME = 'kango-oshigoto'
CSV_FILE_PATH = 'data/kango-oshigoto.csv'

# Initialize Elasticsearch
es = Elasticsearch('localhost:9200')
try:
	es.indices.delete(INDEX_NAME)
except:
	pass

# Open csv file
df = pd.read_csv(CSV_FILE_PATH)
fields = df.columns

# Mapping


mappings = {
    "template": "*",
    "mappings": {
        "_default_": {
            "properties" : {
                "URL" : { "type" : "string", "index" : "not_analyzed" },
                "hello_work" : { "type" : "boolean"},
                "pref" : { "type" : "string", "index" : "not_analyzed" },
            }
        }
    }
}

# Create index
es.indices.create(index=INDEX_NAME, body=mappings)

# Import data
for _, row in df.iterrows():
	data_dict = {}
	for f in fields:
		data_dict[f] = row[f]
	es.index(index=INDEX_NAME, doc_type='test', body=data_dict)

