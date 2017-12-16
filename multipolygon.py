import fiona
from shapely.geometry import shape,mapping, Point, Polygon, MultiPolygon
import csv
from pyspark.sql import SparkSession
import time
import json

multipoly = fiona.open("data/boston_censustracts.json")

#for multi in multipol:
#    if int(multi['id']) - int(multi['properties']['MOVEMENT_ID']) != -1:
#        print (int(multi['id']), int(multi['properties']['MOVEMENT_ID']))

multipoly = [multi for multi in multipoly]

fp = open("data/crime_data.csv")
points = csv.reader(fp)
next(points)
points = [point for point in points]

spark = SparkSession \
    .builder \
    .appName("multipoloygon") \
    .getOrCreate()
sc = spark.sparkContext


def check(polygon, points):
    polygon["crime"] = {}
    polygon["crime"]["hour"] = [0] * 25
    polygon["crime"]["day"] = {"Monday": 0,
                               "Tuesday": 0,
                               "Wednesday": 0,
                               "Thursday": 0,
                               "Friday": 0,
                               "Saturday": 0,
                               "Sunday": 0}
    polygon["crime"]["month"] = [0] * 13
    for point in points:
        coordinate = Point(float(point[-1]), float(point[-2]))
        if coordinate.within(shape(polygon['geometry'])):
            polygon["crime"]["month"][int(point[1])] += 1
            polygon["crime"]["day"][point[2]] += 1
            polygon["crime"]["hour"][int(point[3])] += 1

    return polygon


multipoly = sc.parallelize(multipoly)

points = sc.broadcast(points)

start_time = time.time()
total = multipoly.map(lambda x: check(x, points.value)).collect()
print(time.time() - start_time)

fp = open("data/crime_combined.txt", "w")

for x in total:
    y = {}
    y["id"] = int(x["id"]) + 1
    y["crime"] = x["crime"]
    fp.write(json.dumps(y) + "\n")
    #fp.write(str(int(x["id"]) + 1) + "\n")




