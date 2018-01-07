import fiona
from shapely.geometry import shape, mapping, Point, Polygon, MultiPolygon
import csv
from pyspark.sql import SparkSession
import time
import json

multipoly = fiona.open("data/boston_censustracts.json")

# for multi in multipol:
#    if int(multi['id']) - int(multi['properties']['MOVEMENT_ID']) != -1:
#        print (int(multi['id']), int(multi['properties']['MOVEMENT_ID']))

multipoly = [multi for multi in multipoly]

'''
# fp = open("data/crime_data.csv")
fp = open("data/Police_stations_locations.csv")
points = csv.reader(fp)
next(points)
points = [point for point in points]

fp = open("data/Fire_Departments.csv")
points_fire = csv.reader(fp, delimiter='\t')
next(points_fire)
points_fire = [point for point in points_fire]

fp = open("data/hospital.csv")
points_hospital = csv.reader(fp)
next(points_hospital)
points_hospital = [point for point in points_hospital]
'''
fp = open("data/fire/final-months-0-11.csv")

points = csv.reader(fp)
next(points)
points = [point for point in points]



spark = SparkSession \
    .builder \
    .appName("multipoloygon") \
    .getOrCreate()
sc = spark.sparkContext


def crime(polygon, points):
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

def fire(polygon, points):
    polygon["fire"] = {}
    polygon["fire"]["hour"] = [0] * 25
    polygon["fire"]["day"] = {"Monday": 0,
                               "Tuesday": 0,
                               "Wednesday": 0,
                               "Thursday": 0,
                               "Friday": 0,
                               "Saturday": 0,
                               "Sunday": 0}
    polygon["fire"]["month"] = [0] * 13
    for point in points:
        coordinate = Point(float(point[-1]), float(point[-2]))
        if coordinate.within(shape(polygon['geometry'])):
            polygon["fire"]["month"][int(point[3])] += 1
            polygon["fire"]["day"][point[2]] += 1
            polygon["fire"]["hour"][int(point[1])] += 1

    return polygon


def is_police(polygon, points_police, points_fire, points_hospital):
    polygon["police"] = False
    polygon["fire_stations"] = False
    polygon["hospital"] = False

    for point in points_police:
        coordinate = Point(float(point[-1]), float(point[-2]))
        if coordinate.within(shape(polygon['geometry'])):
            polygon["police"] = True

    for point in points_fire:
        coordinate = Point(float(point[-1]), float(point[-2]))
        if coordinate.within(shape(polygon['geometry'])):
            polygon["fire_stations"] = True


    for point in points_hospital:
        coordinate = Point(float(point[-1]), float(point[-2]))
        if coordinate.within(shape(polygon['geometry'])):
            polygon["hospital"] = True

    return polygon


multipoly = sc.parallelize(multipoly)

points = sc.broadcast(points)
#points_fire = sc.broadcast(points_fire)
#points_hospital = sc.broadcast(points_hospital)

start_time = time.time()
#total = multipoly.map(lambda x: is_police(x, points.value, points_fire.value, points_hospital.value)).collect()
total = multipoly.map(lambda x: fire(x, points.value)).collect()
print(time.time() - start_time)

# fp = open("data/crime_combined.txt", "w")
fp = open("data/fire_combined.txt", "w")

for x in total:
    y = {}
    y["id"] = int(x["id"]) + 1
    #y["police"] = x["police"]
    #y["fire_stations"] = x["fire_stations"]
    #y["hospital"] = x["hospital"]
    y["fire"] = x["fire"]
    fp.write(json.dumps(y) + "\n")
    # fp.write(str(int(x["id"]) + 1) + "\n")
