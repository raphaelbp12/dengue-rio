# -*- coding: utf-8 -*-
# run with pyspark --packages com.databricks:spark-csv_2.10:1.2.0 bigData.py

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sqlite3
from generateInsert import *
from getRddBairros import *
from getBairrosDict import getBairrosDict
from getBairroFromLatLong import *
from getBairroFromLatLongDict import *
import pprint, operator, os, time
from itertools import count
import requests


os.system('clear')

def log(msg):
    print time.strftime("%d/%m/%Y %H:%M:%S >> ", time.localtime()) + msg

def getAnoMesTupleFromFilename(fileName):
    list = fileName.replace(".csv", "").split("_")[3:]
    return (str(list[0]) + str(list[1]), fileName)

start_time = time.time()
log("Inicio Execução Script" )

log("Iniciando verificação do Banco de Dados")
conn = sqlite3.connect('dados.db')
conn.execute("DROP TABLE arquivosImportados") #comentar para reaproveitar dados
conn.execute("DROP TABLE casos") #comentar para reaproveitar dados

cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='arquivosImportados'");
if len(cursor.fetchall()) == 0:
    log("Tabela 'arquivosImportados' não encontrada. Criando tabela...")
    conn.execute('''CREATE TABLE arquivosImportados
                (file CHAR(32) PRIMARY KEY NOT NULL);''')
    log("Tabela 'arquivosImportados' criada com sucesso!\n")
else:
    cursor = conn.execute("SELECT count(*) FROM arquivosImportados");
    log("Tabela 'arquivosImportados' encontrada com sucesso. " + str(cursor.fetchone()[0]) + " registros existentes.")

arquivosImportados = []
cursor = conn.execute("SELECT file FROM arquivosImportados");
for fileLine in cursor.fetchall():
    fileN = str(fileLine[0])
    arquivosImportados.append(fileN)
log(str(len(arquivosImportados)) + " registros importados com sucesso!")


log("Iniciando download arquivos...")

HEADERS = {'user-agent': 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'}
URL = "http://dadosabertos.rio.rj.gov.br/apiSaude/Apresentacao/csv/dengue/Casos_Notificados_Dengue_mes_ano.csv"

session = requests.session()
contador = 0
for y in range(2010, time.localtime()[0]):
    for m in range(1, 13):
        full_url = "http://dadosabertos.rio.rj.gov.br/apiSaude/Apresentacao/csv/dengue/Casos_Notificados_Dengue_mes_ano.csv"

        full_url = full_url.replace("mes", "%02d" % m)
        full_url = full_url.replace("ano", str(y))

        ignored, filename = full_url.rsplit('/', 1)

        if filename not in arquivosImportados:
            log("Baixando: " + filename)
            with file(filename, 'wb') as outfile:
                response = session.get(full_url, headers=HEADERS)
                if response.ok:
                    print len(response.content)
                    outfile.write(response.content[8:]) #remover cedilha
                    contador += 1

log(str(contador) +  " novos arquivos baixados com sucesso! ")

cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='casos'");
if len(cursor.fetchall()) == 0:
    log("Tabela 'casos' não encontrada. Criando tabela...")
    conn.execute('''CREATE TABLE casos
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 dia CHAR(10) NOT NULL,
                 qtd INT ,
                 bairro CHAR(20));''')
    log("Tabela 'casos' criada com sucesso!\n")
else:
    cursor = conn.execute("SELECT count(*) FROM casos");
    log("Tabela 'casos' encontrada com sucesso. " + str(len(cursor.fetchall())) + " registros existentes.")


sc = SparkContext()

sqlContext = SQLContext(sc)

field = [StructField("DT_NOTIFIC", StringType(), True),
         StructField("NU_ANO", IntegerType(), True),
         StructField("Long_WGS84", StringType(), True),
         StructField("Lat_WGS84", StringType(), True), ]
schema = StructType(field)

df = sqlContext.createDataFrame(sc.emptyRDD(), schema)

log("Iniciando geração dicionário bairros...")
bairrosDict = getBairrosDict(sc)
log("Sucesso\n")

cols = ['DT_NOTIFIC', 'NU_ANO', 'Long_WGS84', 'Lat_WGS84']

log("Lendo dados chamados dengue...")
for y in range(2010, time.localtime()[0]):
    for m in range(1, 13):

        fileN = ("Casos_Notificados_Dengue_mes_ano.csv").replace("ano", str(y)).replace("mes", "%02d" % m)
        if fileN not in arquivosImportados:
            log("Processando: " + fileN)
            path = "./" + fileN
            with file(path, 'r') as readfile:
                if len(readfile.read()) == 0:
                    print " 0 casos para o ano de " + str(y) + " mes " + "%02d" % m
                    continue


            dfTemp = sqlContext.read.load(path,
                                          format='com.databricks.spark.csv',
                                          header='true',
                                          inferSchema='true')

            dfTemp = dfTemp.select(cols)
            dfTemp = dfTemp.withColumn('NU_ANO', dfTemp.NU_ANO.cast('int'))

            print " " + str(dfTemp.count()) + " casos para o ano de " + str(y) + " mes " + "%02d" % m
            df = df.unionAll(dfTemp)
            conn.execute("INSERT INTO arquivosImportados (file) VALUES ('" + fileN + "')");

log(str(df.count()) + " novos casos lidos com sucesso!\n")


myRdd = df.rdd
log("Calculando bairro de cada chamado...")
myMappedRdd = myRdd.map(lambda l: (getBairroFromLatLongDict(bairrosDict, l["Lat_WGS84"], l["Long_WGS84"]) + '||' + l["DT_NOTIFIC"].split(' ')[0]))
log(str(myMappedRdd.count()) + " chamados mapeados com sucesso\n")

log("Agrupando chamados por bairro e data...")
myDict = myMappedRdd.countByValue()
log("Agrupamento feito com sucesso!\n")

log("Inserindo dados no banco de dados...")
for key, value in myDict.iteritems():
    conn.execute(generateInsert(key.split('||')[1], value, key.split('||')[0]))

conn.commit()
log("Dados inseridos com sucesso\n")

cursor = conn.execute("SELECT SUM(qtd) FROM casos")
for row in cursor:
    print row[0]

elapsed = start_time - time.time()
log("Fim Execução Script: " + time.strftime('%H:%M:%S', time.gmtime(elapsed)))

