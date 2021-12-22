import pandas as pd
import os
from elasticsearch import Elasticsearch
import elasticsearch.helpers

import dask.dataframe as dd


dados = dd.read_csv('part-00000-a301a49c-59a7-47e0-b1fa-ea644f17f7dd.c000.csv', sep = ';',usecols=['document_id','data_importacao_rnds'],dtype=str)
maior = dados['data_importacao_rnds'].max().compute()
print(maior)
print(dados[dados['data_importacao_rnds'] == maior].compute())
