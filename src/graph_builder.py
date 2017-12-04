import glob
import json
import gzip
import tarfile
from igraph import *
from pprint import pprint
import pandas as pd
import sys
from pyproj import Proj, transform

inProj = Proj(init='epsg:4326')
outProj = Proj(init='epsg:27700')

def functioner(file_name):

	tar = tarfile.open(file_name, "r:gz")
	dest_folder = "../tmp/" + file_name[6:-7]
	tar.extractall(dest_folder)
	tar.close()

	os.chdir(dest_folder + "/gtfs/")

	nodes_data = []
	address_data = []

	for file in glob.glob("*.txt"):
		if file == "stops.txt":

			print "Extracting stops"
			stops_data = pd.read_csv(file)
			print len(stops_data), " stops laoded"

			stops_data_dict_raw = stops_data.to_json(orient='records')
			stops_data_dict = json.loads(stops_data_dict_raw)

			for stop in stops_data_dict:
				
				node_data = {
				"index" : 0,
				"toid" : stop['stop_id'] ,
				# Convert coordinate system
				"point" : transform(inProj,outProj,stop['stop_lon'],stop['stop_lat'])
				}
				
				nodes_data.append(node_data)

				add_data = {
				"toid" : stop['stop_id'],
				"text" : stop['stop_name']
				}
				
				address_data.append(add_data)

	# Set index of nodes_data
	for count, node in enumerate(nodes_data, start=0):
		node['index'] = count

	# Dump to JSON
	with gzip.open("../../../out/nodes1.json.gz",'w+') as outfile:
		json.dump(nodes_data,outfile,indent=2)
	
	with gzip.open("../../../out/addresses.json.gz",'w+') as outfile:
		json.dump(address_data,outfile,indent=2)

	# "index": 0,
	# "term": "A Road",
	# "restriction": "One Way",
	# "nature": "Roundabout",
	# "negativeNode": "osgb4000000023182261",
	# "toid": "osgb4000000023429135",
	# "polyline": [
	# 499766.775,
	# 151379.967,
	# 499766,
	# 151381,
	# 499763,
	# 151385,
	# 499760,
	# 151387,
	# 499756,
	# 151389,
	# 499751,
	# 151389,
	# 499750,
	# 151389
	# ],
	# "positiveNode": "osgb4000000023182244",
	# "orientation": "-"
	# },



functioner("../in/tfl_gtfs_2016-12-01_05.tar.gz")