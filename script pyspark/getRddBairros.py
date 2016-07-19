from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sqlite3

def getRddBairros(sc):

    sqlContextB = SQLContext(sc)

    pathB = "./bairros_utf8.csv"

    dfB = sqlContextB.read.load(pathB, format='com.databricks.spark.csv', header='true', inferSchema='true')

    print 'Quantidade de Bairros: ' + str(dfB.count())

    myRddB = dfB.rdd
    return myRddB