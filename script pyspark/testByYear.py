#http://www.nodalpoint.com/dataframes-from-csv-files-in-spark-1-5-automatic-schema-extraction-neat-summary-statistics-elementary-data-exploration/

#http://www.nodalpoint.com/spark-data-frames-from-csv-files-handling-headers-column-types/

#run with pyspark --packages com.databricks:spark-csv_2.10:1.2.0 testByYear.py

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sqlite3



def generateInsert(data, qtd, bairro):
	qry = "INSERT INTO casos VALUES ('" + data + "', " + str(qtd) + ", '" + bairro + "')"
	return qry


conn = sqlite3.connect('dados.db')
conn.execute('''CREATE TABLE casos
		(dia CHAR(10) PRIMARY KEY NOT NULL,
		 qtd INT ,
		 bairro CHAR(20));''')

sc = SparkContext()

sqlContext = SQLContext(sc)


#field = [StructField("NU_ANO", IntegerType(), True) ]
#field = [StructField("DT_NOTIFIC", TimestampType(), True),
field = [StructField("DT_NOTIFIC", StringType(), True),
	 StructField("NU_ANO", IntegerType(), True),    
	 StructField("Long_WGS84", LongType(), True),
	 StructField("Lat_WGS84", LongType(), True),]
schema = StructType(field)

df = sqlContext.createDataFrame(sc.emptyRDD(), schema)

cols = ['DT_NOTIFIC', 'NU_ANO','Long_WGS84', 'Lat_WGS84']


for y in range(2010,2015):
#for y in range(2010,2011):
	path = ("./Casos_Notificados_Dengue_*_ano.csv").replace("ano", str(y))
	dfTemp = sqlContext.read.load(path, 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')
	#print dfTemp.count()
	#dfTemp = dfTemp.select('NU_ANO')
	dfTemp = dfTemp.select(cols)
	dfTemp = dfTemp.withColumn('NU_ANO', dfTemp.NU_ANO.cast('int'))
	#dfTemp = dfTemp.withColumn('DT_NOTIFIC', dfTemp.DT_NOTIFIC.cast('timestamp'))
	dfTemp = dfTemp.withColumn('Long_WGS84', dfTemp.DT_NOTIFIC.cast('long'))
	dfTemp = dfTemp.withColumn('Lat_WGS84', dfTemp.DT_NOTIFIC.cast('long'))
	#print dfTemp.dtypes
	#print df.dtypes		
	df = df.unionAll(dfTemp)
	print df.count()

myRdd = df.rdd
#myMappedRdd = myRdd.map(lambda l: (l["NU_ANO"],1))
myMappedRdd = myRdd.map(lambda l: (l["DT_NOTIFIC"].split(' ')[0]))
myDict = myMappedRdd.countByValue()


for key, value in myDict.iteritems():
	#query = "INSERT INTO casos VALUES ('" + key + "', " + str(value) + ", 'RIO')"
	#print query
	#conn.execute(query)
	conn.execute(generateInsert(key, value, "RIO"))

conn.commit()




#df.registerTempTable("dataSource")


#sqlContext.sql("SELECT NU_ANO, COUNT(*) FROM dataSource GROUP BY NU_ANO").show()
#sqlContext.sql("SELECT DT_NOTIFIC, COUNT(*) FROM dataSource GROUP BY DT_NOTIFIC ").show()

	

'''

df10 = sqlContext.read.load('./Casos_Notificados_Dengue_*_2010.csv', 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')

print df10.count()

df10 = df10.select('NU_ANO')

df11 = sqlContext.read.load('./Casos_Notificados_Dengue_*_2011.csv', 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')

print df11.count()
df11 = df11.select('NU_ANO')

df = df10.unionAll(df11)

print df.count()'''
'''

'a = df.count()



df = df.withColumn('NU_ANO', df.NU_ANO.cast('int'))
df.registerTempTable("dataSource")

sqlContext.sql("SELECT NU_ANO, COUNT(*) FROM dataSource WHERE NU_ANO > 2009 AND NU_ANO < 2015 GROUP BY NU_ANO").show()



#df.describe().show()

'''



