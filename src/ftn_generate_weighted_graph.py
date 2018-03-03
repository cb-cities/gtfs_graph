import gzip
import time
from pprint import pprint
import pandas as pd
import glob
import ujson as json
import sys
import pandas as pd
from pyproj import Proj, transform

inProj = Proj(init='epsg:27700')
outProj = Proj(init='epsg:4326')

def file_dumper(data, name,chunkSize):

	print "Writing ", name, " to file"
	
	for i in xrange(0, len(data), chunkSize):
		with gzip.open('../out/graph/' + name + str((i//chunkSize)+1) + '.json.gz', 'w') as outfile:
			json.dump(data[i:i+chunkSize], outfile, indent =2)

def polyline_generator(negativeNode,positiveNode,stops_db):

	# This simply creates a straightline between negative and positive nodes
	# Inclusion of real-world polyline would include some pairing to OSM

	polyline = []

	start = transform(outProj,inProj,stops_db[negativeNode]['stop_lon'],stops_db[negativeNode]['stop_lat'])
	end = transform(outProj,inProj,stops_db[positiveNode]['stop_lon'],stops_db[positiveNode]['stop_lat'])
	polyline.extend(start)
	polyline.extend(end)

	return polyline

def gen_edges(unique_edges,stops_db):

	print len(unique_edges), " edges found"
	
	gen_edges = []

	for edge in unique_edges:
		edge_id = edge['service_id']
		negativeNode = edge['records'][0]['negativeNode']
		positiveNode = edge['records'][0]['positiveNode']
		weight_data = []
		
		for record in edge['records']:

			route_type = record['service_information']['route_type']
			route_id = record['service_information']['route_id']
			route_agency = record['service_information']['agency']
			
			modes = set()
			jts = []
			
			if len(record['services']) != 0:
				
				for service in record['services']:
					data = {
						"departure_time" : service['departure_time'],
						"arrival_time" : service['arrival_time'],
						"journey_time" : service['journey_time']
					}
					jts.append(service['journey_time'])

					data['route_type'] = route_type
					data['route_id'] = route_id
					data['route_agency'] = route_agency
					
					# Check - surely this was always return a single value?
					modes.add(route_type)
					weight_data.append(data)
					
			average_jt = sum(jts) / float(len(record['services']))

		# Order services by departure_time
		ordered_weight_data = sorted(weight_data, key=lambda k: k['departure_time'])
			
		output = {
			# "toid" : edge_id,
			"edge_id" : edge_id,
			"graph" : "GTFS",
			"index" : 0,
			"negativeNode" : negativeNode,
			"positiveNode" : positiveNode,
			"polyline" : polyline_generator(negativeNode,positiveNode,stops_db),
			"services" : ordered_weight_data,
			"journey_time" : average_jt,
			'modes' : list(modes)
		}
		
		gen_edges.append(output)

	return gen_edges

def gen_nodes(stops_db):
	
	nodes_data = []
	address_data = []
	
	for key,val in stops_db.iteritems():
		conv_point = (val['stop_lat'],val['stop_lon'])
		# conv_point = transform(outProj,inProj,val['stop_lon'],val['stop_lat'])
			
		node_data = {
			
			"index" : 0,
			"toid" : val['stop_id'] ,
			"point" : conv_point
		
		}
		
		nodes_data.append(node_data)
		
		add_data = {

			"toid" : val['stop_id'],
			"text" : val['stop_name']
		}
		
		address_data.append(add_data)

	print "Re-indexing..."

	for count, node in enumerate(nodes_data, start=0):
		node['index'] = count

	for count, add_data in enumerate(address_data, start=0):
		add_data['index'] = count

	return nodes_data, address_data