from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sqlite3
import pprint

dict = {}


def getBairrosDict(sc):

    sqlContextB = SQLContext(sc)

    pathB = "./bairros_utf8.csv"

    dfB = sqlContextB.read.load(pathB, format='com.databricks.spark.csv', header='true', inferSchema='true')

    myRddB = dfB.rdd

    return myRddB.map(lambda x: ((x["Latitude"], x["Longitude"]), x["Bairro"])).collectAsMap()

def addToDict(x):
    global dict
    dict[(x["Latitude"], x["Longitude"])] = x["Bairro"]
