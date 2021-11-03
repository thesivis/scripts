import pandas as pd


dados = pd.read_csv('dados.csv', sep = ';',dtype = {'cnpj': str,'simples': str})
calculado = 0
nCalculado = 0
for idx,row in dados.iterrows():
	cnpj = row['cnpj']
	if(len(cnpj) > 11):
		if(type(row['simples']) == float):
			nCalculado = nCalculado + 1
		else:
			calculado = calculado + 1
			
print('Total:',(calculado+nCalculado))
print('Calculado:',calculado,calculado/(calculado+nCalculado)*100)
print('Nao Calculado:',nCalculado,nCalculado/(calculado+nCalculado)*100)
