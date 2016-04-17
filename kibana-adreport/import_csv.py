"""
Import csv data and create index to elasticsearch
This code is prototype
"""

import pandas as pd
from elasticsearch import Elasticsearch

# Initialize Elasticsearch
es = Elasticsearch('localhost:9200')
try:
	es.indices.delete('ad_reports')
except:
	pass

df = pd.read_csv('ENTER_CSV_FILEPATH_TO_BE_IMPORTED ')
fields = df.columns

for _, row in df.iterrows():
	data_dict = {}
	for f in fields:
		data_dict[f] = row[f]
	es.index(index='ad_reports', doc_type='test', body=data_dict) 




