import csv
import json
fp = open("data/crime_combined.txt", "r")

y = {}
for x in fp:
    x = json.loads(x)
    res = [0]*7

    for k,v in x["crime"]["day"].items():
        if k == "Sunday":
            res[0] = v
        elif k == "Monday":
            res[1] = v
        elif k == "Tuesday":
            res[2] = v
        elif k == "Wednesday":
            res[3] = v
        elif k == "Thursday":
            res[4] = v
        elif k == "Friday":
            res[5] = v
        elif k == "Saturday":
            res[6] = v
    res += x["crime"]["hour"][:24]
    res += x["crime"]["month"][1:]
    if len(res) != 43:
        print(len(res))
    y[x["id"]] = res


fp = open("data/fire_combined.txt", "r")

for x in fp:
    x = json.loads(x)
    res = [0]*7

    for k,v in x["fire"]["day"].items():
        if k == "Sunday":
            res[0] = v
        elif k == "Monday":
            res[1] = v
        elif k == "Tuesday":
            res[2] = v
        elif k == "Wednesday":
            res[3] = v
        elif k == "Thursday":
            res[4] = v
        elif k == "Friday":
            res[5] = v
        elif k == "Saturday":
            res[6] = v
    res += x["fire"]["hour"][:24]
    res += x["fire"]["month"][1:]
    if len(res) != 43:
        print(len(res))
    y[x["id"]] += res




fp = open("data/final_join.csv", "r")
next(fp)
for line in fp:
    line = line.strip()
    #line = line[1:-1].split(",")
    line = line.split(",")
    #print(len(line))
    y[int(float(line[0]))] += line[1:]

fw = open("data/Final_col.txt", "w")

padding = [0] * (6* (24+7+12))
for k,v in sorted(y.items(), key=lambda x: x[0]):
    #print(len(v))
    if len(v) < (6* (24+7+12)):
        for e in v:
            e = str(e) + ","
            fw.write(e)
        for e in padding:
            e = str(e) + ","
            fw.write(e)

    else:
        for e in v:
            e = str(e) + ","
            fw.write(e)
    fw.write("\n")