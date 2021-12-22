import pandas as pd
from elasticsearch import Elasticsearch
import elasticsearch.helpers

user = 'imunizacao_public'
pwd = 'qlto5t&7r_@+#Tlstigi'
timeout = 3000
scroll = '50m'

url = {
    'host' : 'imunizacao-es.saude.gov.br',
    'port' : '443',
    'scheme':'https',
    'use_ssl' : True,
    'http_auth' : (user, pwd),
    'path' : '_search'
}
print(url)
es = Elasticsearch([url], send_get_body_as='GET', timeout=timeout)
body={
	"query": {
		"match": {
			"paciente_endereco_uf":"MT"
		}
	}
}
print(es)
results = elasticsearch.helpers.scan(es, query=body,scroll=scroll,size='10000',request_timeout=timeout)
i = 0
for document in results:
    if(i % 100000 == 0):
        print(i)
    df = pd.DataFrame.from_dict([document['_source']])
    if(i >= 1):
        df.to_csv('vacina.csv', sep = ';', encoding='utf-8-sig', index = False, mode='a', header=False)
    else:
        df.to_csv('vacina.csv', sep = ';', encoding='utf-8-sig', index = False)
    i = i + 1
