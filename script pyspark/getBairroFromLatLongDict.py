from decimal import *
from haversine import *


def getBairroFromLatLongDict(dictB, Lat, Long):

    dLat = Decimal(Lat)
    dLong = Decimal(Long)

    minValue = Decimal('inf')
    minBairro = "None"

    for key, value in dictB.iteritems():
        dist = haversine(dLong, dLat, Decimal(key[1]), Decimal(key[0]))
        if dist < minValue:
            minValue = dist
            minBairro = value

    return minBairro

