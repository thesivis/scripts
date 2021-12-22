import pandas as pd
import os
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import numpy as np
import dask.dataframe as dd


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
colunas = ["vacina_categoria_codigo", "estalecimento_noFantasia", "paciente_endereco_coPais", "vacina_lote", "document_id", "paciente_endereco_nmPais", "vacina_fabricante_referencia", "vacina_nome", "estabelecimento_valor", "@timestamp", "paciente_dataNascimento", "estabelecimento_razaoSocial", "paciente_endereco_nmMunicipio", "vacina_descricao_dose", "vacina_dataAplicacao", "paciente_nacionalidade_enumNacionalidade", "estabelecimento_uf", "vacina_grupoAtendimento_codigo", "vacina_grupoAtendimento_nome", "vacina_fabricante_nome", "vacina_codigo", "status", "paciente_endereco_coIbgeMunicipio", "id_sistema_origem", "sistema_origem", "paciente_endereco_uf", "paciente_idade", "paciente_racaCor_valor", "paciente_racaCor_codigo", "paciente_enumSexoBiologico", "data_importacao_rnds", "@version", "dt_deleted", "estabelecimento_municipio_codigo", "paciente_id", "estabelecimento_municipio_nome", "paciente_endereco_cep", "vacina_categoria_nome"]
time = None
if(os.path.exists("vacina.csv")):
    dados = dd.read_csv('vacina.csv', sep = ';',usecols=['@timestamp'],dtype=str)
    time = dados['@timestamp'].max().compute()
print(time)
if(time == None):
    df = pd.DataFrame([],columns=colunas)
    df.to_csv('vacina.csv', sep = ';', encoding='utf-8-sig', index = False)
    body={
        "query": {
            "match": {"estabelecimento_uf":"MT"}
        },
        "sort":[
            {"@timestamp":"asc"}
        ]
    }
else:
    body={
        "query": {
            "bool":{
              "must":[
                {"match": {"estabelecimento_uf":"MT"}},
                {"range" : {"@timestamp" : { "gt" : str(time)}}}
              ]
            }
        },
        "sort":[
            {"@timestamp":"asc"}
        ]
    }
print(es)
print(body)
results = elasticsearch.helpers.scan(es, query=body,scroll=scroll,size=10000,request_timeout=timeout,preserve_order=True,sort=["@timestamp"])
i = 0

for document in results:
    if(i % 100000 == 0):
        print(i)

    df = pd.DataFrame([],columns=colunas)
    df = df.append(document['_source'],ignore_index=True)
    df.to_csv('vacina.csv', sep = ';', encoding='utf-8-sig', index = False, mode='a', header=False)
    i = i + 1
