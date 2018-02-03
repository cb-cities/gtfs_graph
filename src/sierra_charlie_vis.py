import sys
from pprint import pprint
import gzip
import json
from pyproj import Proj, transform
import glob
import os

outProj = Proj(init='epsg:27700')
inProj = Proj(init='epsg:4326')

all_modes_list = [u'Bus', u'Subway, Metro', u'Tram, Streetcar, Light rail', u'Ferry']

def convert_points(east_north):

	return transform(inProj,outProj,east_north[0],east_north[1])

nodes_db = {}

for mode in all_modes_list:
	
	print "Working on mode: ", mode
	
	for file in glob.glob("../out/Vis/*"+ mode + "*"):
		name = file[file.rfind("/")+1:file.rfind("_")]
		data = json.load(gzip.open(file))
		if not os.path.exists("../out/sierra-charlie/"+mode+"/"):
				os.makedirs("../out/sierra-charlie/"+mode+"/")
		
		if name == "nodes":
			for node in data:
				point = (node['point'][1], node['point'][0])
				node['point'] = convert_points(point)
				nodes_db[node['toid']] = node

			for count, record in enumerate(data, start=0):
				record['index'] = count

			print "Dumping nodes"
			with gzip.open("../out/sierra-charlie/"+mode+"/nodes1.json.gz",'w') as outfile:
				json.dump(data,outfile,indent=2)


for mode in all_modes_list:
	
	print "Working on mode: ", mode
	
	for file in glob.glob("../out/Vis/*"+ mode + "*"):
		name = file[file.rfind("/")+1:file.rfind("_")]

		if name == "links":
			data = json.load(gzip.open(file))
			for record in data:
				record['toid'] = record['edge_id']
				record.pop("edge_id")
				record['polyline'] = []
				record['polyline'].extend(nodes_db[record['negativeNode']]['point'])
				record['polyline'].extend(nodes_db[record['positiveNode']]['point'])
				record['index'] = 0

			for count, record in enumerate(data, start=0):
				record['index'] = count
				
			print "Dumping links"
			with gzip.open("../out/sierra-charlie/"+mode+"/links1.json.gz",'w') as outfile:
				json.dump(data,outfile,indent=2)

