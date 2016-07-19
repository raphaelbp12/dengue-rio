#http://www.nodalpoint.com/dataframes-from-csv-files-in-spark-1-5-automatic-schema-extraction-neat-summary-statistics-elementary-data-exploration/

#http://www.nodalpoint.com/spark-data-frames-from-csv-files-handling-headers-column-types/

#run with pyspark --packages com.databricks:spark-csv_2.10:1.2.0 test.py

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sc =SparkContext()

sqlContext = SQLContext(sc)

df = sqlContext.read.load('./Casos_Notificados_Dengue_*_2010.csv', 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')




a = df.count()
print a


df = df.withColumn('NU_ANO', df.NU_ANO.cast('int'))
df.registerTempTable("dataSource")

sqlContext.sql("SELECT NU_ANO, COUNT(*) FROM dataSource WHERE NU_ANO > 2009 AND NU_ANO < 2015 GROUP BY NU_ANO").show()



#df.describe().show()



