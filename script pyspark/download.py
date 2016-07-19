from itertools import count
import requests

HEADERS = {'user-agent': 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'}
#URL = "http://www.example.com/img%03d.png"
URL = "http://dadosabertos.rio.rj.gov.br/apiSaude/Apresentacao/csv/dengue/Casos_Notificados_Dengue_mes_ano.csv"

#ignored, filename = URL.rsplit('/', 1)

# with a session, we get keep alive
session = requests.session()

for y in range(2010,2015):
	for m in range(1,13):
		full_url = "http://dadosabertos.rio.rj.gov.br/apiSaude/Apresentacao/csv/dengue/Casos_Notificados_Dengue_mes_ano.csv"

		full_url = full_url.replace("mes", "%02d" % m)
		full_url = full_url.replace("ano", str(y))

		
		ignored, filename = full_url.rsplit('/', 1)
		print filename

		with file(filename, 'wb') as outfile:
			response = session.get(full_url, headers=HEADERS)
			if response.ok:
				outfile.write(response.content)
				'''with open(filename, 'r') as fin:
					data = fin.read().splitlines(True)
					print len(data[1].split(","))
				with open(filename, 'r') as fin:
				    data = fin.read().splitlines(True)
				with open(filename, 'w') as fout:
				    fout.writelines(data[1:])'''
