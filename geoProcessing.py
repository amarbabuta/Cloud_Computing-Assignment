#!/usr/bin/env python
# coding: utf-8

import json
import numpy as np
from mpi4py import MPI
from heapq import nlargest
from operator import itemgetter
from shapely.geometry.polygon import Polygon
from shapely.geometry import Point
from collections import Counter
from timeit import default_timer as timer

start=timer()

boxes = ['A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4', 'C1', 'C2', 'C3', 'C4', 'C5', 'D3', 'D4', 'D5', 'None']

comm=MPI.COMM_WORLD
rank=comm.Get_rank()
size=comm.Get_size()

def grid_activity(data):
    grid_value=[grid_id(data[j][0],data[j][1]) for j in range(len(data))]
    grid_counts=Counter(grid_value).most_common()
    return grid_counts,grid_value
#print(grid_activity)

def grid_id(lat,long):
    point=Point(lat,long)
    for i in grid_poly:
        try:
            if point.within(grid_poly[i]) or point.intersects(grid_poly[i]):
                return i
        except:
            print("Invalid point")
            return None
    return None

with open("/data/projects/COMP90024/melbGrid.json","r") as f:
    melb_grid=json.load(f)

grid_coordinates={}
for feature in melb_grid["features"]:
    grid_coordinates[feature["properties"]["id"]]=feature["geometry"]["coordinates"]
#print(grid_coordinates)

polygon_coordinates={}
for box in grid_coordinates:
    coords_x=[]
    coords_y=[]
    for pointsList in grid_coordinates[box]:
        for eachPoint in pointsList:
            coords_x.append(eachPoint[1])
            coords_y.append(eachPoint[0])
    polygon_coordinates[box]=[np.array(coords_x),np.array(coords_y)]
#print(polygon_coordinates)


grid_poly={}
for box in polygon_coordinates:
    grid_poly[box]=Polygon(np.column_stack((polygon_coordinates[box][0],polygon_coordinates[box][1])))
#print(grid_poly)


def hash_activity(box_value, box_data):
    box_distinct_data = {str(h): [] for h in set(box_value)}
    for box, data in zip(box_value, box_data):
        box_distinct_data[str(box)] += [data[2]]

    hash_set = {key: [] for key in box_distinct_data.keys()}
    for box in box_distinct_data:
        if box != "None":
            for tweet in box_distinct_data[box]:
                hash_set[box] += hashtags(tweet)

    hash_set_counts = {}

    for box in hash_set:
        hash_set_counts[box] = Counter(hash_set[box]).most_common(5)
    return hash_set_counts
#print(hash_activity)

#Returns hastag text in lower case
def hashtags(txt):
    hash_list=[]
    for k in txt:
        hash_list.extend([k['text'].lower()])
    return set(hash_list)
#print(hastags)


with open("/data/projects/COMP90024/bigTwitter.json", "r",encoding="utf-8") as f1:
    l_r = {l: 1 for l in list(range(0,1000000,size))}
    tweets_info = []

    for num, ex in enumerate(f1):
        if num in l_r:

            try:
                json_data = json.loads(ex.strip()[:-1])
                if 'coordinates' in json_data['doc']:
                    tweets_info.append([json_data['doc']['coordinates']['coordinates'][1], json_data['doc']['coordinates']['coordinates'][0],
                                         json_data['doc']['entities']['hashtags']])
            except:
                try:
                    json_data = json.loads(ex.strip())
                    tweets_info.append([json_data['doc']['coordinates']['coordinates'][1], json_data['doc']['coordinates']['coordinates'][0],
                                         json_data['doc']['entities']['hashtags']])
                except:
                    pass



grid_counts, box_value = grid_activity(tweets_info)
hashtag_count = hash_activity(box_value, tweets_info)



if rank != 0:
    comm.send([hashtag_count, grid_counts], dest=0)

input = list(range(size))

if rank == 0:
    for u in range(size):
        if u != 0:
            input[u] = comm.recv(source=u)

    input[0] = [hashtag_count, grid_counts]

    dic = {v: [] for v in boxes}
    box_merge = {v: 0 for v in boxes}

    for value in input:
        for w in range(len(value[1])):
            if value[1][w][0] is not None:
                box_merge[value[1][w][0]] += value[1][w][1]

    for values in input:
        for pp in values[0]:
            dic[pp] += values[0][pp]

    combined_dic = {}

    for pp in dic:
        if pp != 'None':
            combined_dic_add = {}
            for f in dic[pp]:
                if f[0] in combined_dic_add:
                    combined_dic_add[f[0]] += f[1]
                else:
                    combined_dic_add[f[0]] = f[1]

            tweet_freq = nlargest(5, combined_dic_add, key=combined_dic_add.__getitem__)
            combined_dic[pp] = {count: combined_dic_add[count] for count in tweet_freq}

        #print("Completed\n")
    data = sorted(box_merge.items(), key=itemgetter(1), reverse=True)
    for z in data:
        print('%s : %d' % (z[0], z[1]))

        #print("\n\n\n")
    for l, f in combined_dic.items():
        if l != "None" and f != {}:
            print(l, sorted(f.items(), key=itemgetter(1), reverse=True))



end = timer()
time = end-start

print(time)