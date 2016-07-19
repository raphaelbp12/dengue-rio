from decimal import *
from haversine import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sqlite3

def getBairroFromLatLong(rddB, Lat, Long, sc):

    dLat = Decimal(Lat)
    dLong = Decimal(Long)

    tempRdd = rddB.map(lambda l: (l["Bairro"], haversine(dLong, dLat, Decimal(l["Longitude"]), Decimal(l["Latitude"]))))

    return tempRdd.min()[0]
