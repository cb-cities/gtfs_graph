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

def check_dir(directory):

	if not os.path.exists(directory):
		os.makedirs(directory)

# gen_addresses():
	# njkandks 

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
	rejected_edges = []

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
			
			try:
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
			except Exception as e:
				print e
				pprint(record)
				sys.exit(1)
					
			average_jt = sum(jts) / float(len(record['services']))

		if average_jt > 0:

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
				'modes' : modes
			}
			
			gen_edges.append(output)

		else:

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
				'modes' : modes
			}

			data = {
				"edge_data" : output,
				"rejection_cause" : "journey time less than zero",
				"journey_time" : average_jt
			}

			rejected_edges.append(data)

	pprint(gen_edges[0])

	sys.exit(1)

	chunkSize = 1000
	for i in xrange(0, len(results), chunkSize):
		with gzip.open('../out/graph/gtfs_' + str((i//chunkSize)+1) + '.json.gz', 'w') as outfile:
			json.dump(results[i:i+chunkSize], outfile, indent =2)

	# return gen_edges

def create_nodes(stops_db):
	
	nodes_data = []
	address_data = []
	
	for key,val in stops_db.iteritems():

		point = (val['stop_lat'],val['stop_lon'])
		conv_point = convert_points(point)
			
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

	print "Dumping node data"
	with gzip.open("../out/graph/gtfs_node_data.json.gz",'w') as outfile:
		json.dump(nodes_data,outfile,indent=2)

	print len(nodes_data), " nodes generated"

	print "Dumping address data"
	with gzip.open("../out/graph/gtfs_address_data.json.gz",'w') as outfile:
		json.dump(address_data,outfile,indent=2)

# below for extracting sub graphs

# def extract_nodes(edges):
	
# 	nodes_toid_list = []
# 	for edge in edges:
# 		nodes_toid_list.append(edge['negativeNode'])
# 		nodes_toid_list.append(edge['positiveNode'])

# 	unique_nodes_toid_list = list(set(nodes_toid_list))

# 	tmp_nodes = []
# 	for toid in unique_nodes_toid_list:
# 		node = nodes_db[toid]
# 		tmp_nodes.append(node)

# 	return tmp_nodes

# def filter_mode(record, mode):
	
# 	output = []
# 	for service in record['services']:
# 		if service['route_type'] == mode:
# 			output.append(service)
	
# 	record.pop('services')
# 	record['services'] = output
	
# 	return record

# def filter_graph_modes(mode):

# 	print "extracting records for mode: ", mode
	
# 	tmp_nodes = []
# 	tmp_edges = []

# 	for file in glob.glob("../out/graph/gtfs*edge*.json.gz"):
		
# 		print "Loading", file
# 		data = json.load(gzip.open(file))
		
# 		for record in data:
			
# 			record = filter_mode(record,mode)

# 			# Check the record. append, change or remove other data?

# 			# for record in tmp_edges:
# 			# 	record['toid'] = record['edge_id']
# 			# 	record.pop("edge_id")
# 			# 	record['polyline'] = []
# 			# 	record['polyline'].extend(nodes_db[record['negativeNode']]['point'])
# 			# 	record['polyline'].extend(nodes_db[record['positiveNode']]['point'])
# 			# 	record['index'] = 0

# 			# for count, record in enumerate(tmp_edges, start=0):
# 			# 	record['index'] = count

# 			tmp_edges.append(record)

# 	tmp_nodes = extract_nodes(tmp_edges)

# 	print len(tmp_edges), " edges loaded"
# 	print len(tmp_nodes), " nodes loaded"

# 	print "Dumping nodes"
# 	with gzip.open("../out/sierra-charlie/"+mode+"/nodes1.json.gz",'w') as outfile:
# 		json.dump(data,outfile,indent=2)

# 	print "Dumping links"
# 	with gzip.open("../out/sierra-charlie/"+mode+"/links1.json.gz",'w') as outfile:
# 		json.dump(tmp_edges,outfile,indent=2)