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

fp = open("data/subway/boston_transit_stop.csv")
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

def list_poly(polygon, points):
    poly_list = []
    for point in points:
        coordinate = Point(float(point[-3]), float(point[-4]))
        if coordinate.within(shape(polygon['geometry'])):
            poly_list.append((int(polygon["id"])+1, point))

    return poly_list

multipoly = sc.parallelize(multipoly)

points = sc.broadcast(points)

start_time = time.time()
total = multipoly.map(lambda x: list_poly(x, points.value)) \
				 .flatMap(lambda x: x) \
				 .collect()
print(time.time() - start_time)

fp = open("data/subway/subway_uber.csv", "w")
fp.write("mvmt_id,index,direction,parent_station,parent_station_name,route_id,stop_id,stop_lat,stop_lon,stop_name,stop_order\n")
'''
for x in total:
    y = {}
    y["id"] = int(x["id"]) + 1
    y["crime"] = x["crime"]
    fp.write(json.dumps(y) + "\n")
    #fp.write(str(int(x["id"]) + 1) + "\n")
'''

for i in total:
	fp.write(str(i[0])+','+','.join(i[1])+'\n')


