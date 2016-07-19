from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sqlite3


scB = SparkContext()

sqlContextB = SQLContext(scB)

pathB = "./bairros_.csv"

dfB = sqlContextB.read.load(pathB, format='com.databricks.spark.csv', header='true', inferSchema='true')

print 'Quantidade de Bairros: ' + dfB.count()

myRddB = dfB.rdd


