from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

browser = webdriver.Firefox()
url = "https://consopt.www8.receita.fazenda.gov.br/consultaoptantes"
dados = pd.read_csv('dados.csv', sep = ';',dtype = {'cnpj': str,'simples': str})
erro = 0
for idx,row in dados.iterrows():
	cnpj = row['cnpj']
	if(len(cnpj) > 11 and type(row['simples']) == float):
		if(erro >= 2):
			browser.close()
			time.sleep(30)
			browser = webdriver.Firefox()
		browser.get(url)
		elem = browser.find_element(By.NAME, "Cnpj")
		elem.clear()
		elem.send_keys(cnpj)
		elem.send_keys(Keys.RETURN)
		button = browser.find_element(By.CLASS_NAME, 'btn-verde')
		button.click()
		try:
			
			wait = WebDriverWait(browser, 30)
			element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'panel-title')))

			div = browser.find_element(By.XPATH, "/html/body/div/div[2]/div[2]/div[2]/span[1]")
			print(cnpj,div.text)
			dados.at[idx,'simples'] = div.text
			dados.to_csv('dados.csv',sep=';', index=False)
			erro = 0
		except:
			print('Erro:',erro,cnpj)
			erro = erro + 1
			pass
		time.sleep(3)

browser.close()

