import gzip
import time
from pprint import pprint
import pandas as pd
import glob
import ujson as json
import sys
import pandas as pd
from ggplot import *

def gen_edges(path):
	
	unique_edges = []
	for file in glob.glob(path):
		data = json.load(gzip.open(file))
		unique_edges.extend(data)

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
			try:
				for service in record['services']:
					data = {
					"departure_time" : service['departure_time'],
					"arrival_time" : service['arrival_time'],
					"journey_time" : service['journey_time']
					}

					data['route_type'] = route_type
					data['route_id'] = route_id
					data['route_agency'] = route_agency
					weight_data.append(data)
			except KeyError:
				# Not sure why this is happening.... to come back to..
				for service in record['time_tabled_services']:
					data = {
						"departure_time" : service['departure_time'],
						"arrival_time" : service['arrival_time'],
						"journey_time" : int(service['arrival_time']) - int(service['departure_time'])
					}
					if data['journey_time'] != 0:
						print 'maybe not/...'
						pprint(data)
					data['route_type'] = route_type
					data['route_id'] = route_id
					data['route_agency'] = route_agency
					
					# This appears to be when journey times = 0.... skipping for now...

					# weight_data.append(data)

		# Order services by departure_time
		ordered_weight_data = sorted(weight_data, key=lambda k: k['departure_time'])
		
		output = {
			"edge_id" : edge_id,
			"negativeNode" : negativeNode,
			"positiveNode" : positiveNode,
			"services" : ordered_weight_data
		}
		
		gen_edges.append(output)

	return gen_edges

def headway_calculator(edges):

	headways = []
	for edge in edges:
		
		# Extract service times
		services = edge['services']

		# Order services by departure_time (again, as we may have multiple operators, and routes)
		ordered_services = sorted(services, key=lambda k: k['departure_time'])
		
		for i in range(0,len(ordered_services)-1):
				
			headway = int(ordered_services[i+1]['departure_time']) - int(ordered_services[i]['departure_time']) / 60.0
				
			# 10hrs
			if headway < 60:
				time_stamp = ordered_services[i]['departure_time']
				mode = ordered_services[i]['route_type']
				
				data = {
					"headway" : headway,
					"time_stamp" : time_stamp,
					"mode" : mode
				}
				headways.append(data)

	headways_df = pd.DataFrame(headways)

	# Convert time stamp
	# Add day of week
	# Add hour
	# Create hour to AM, PM and IP ftn
	# Classify AM, PM and IP

	s = ggplot(aes(x="headway",color="mode"),data=headways_df) + geom_density()
	
	s.save("../out/plots/headway_density.png")

generated_edges = gen_edges("../out/unique_edges/*unique_edge*13*.json.gz")

# pprint(generated_edges[0])

headway_calculator(generated_edges)

def create_nodes(stops_db):
	nodes_data = []
	address_data = []
	
	for key,val in stops_db.iteritems():
			
		node_data = {
			
			"index" : 0,
			"toid" : val['stop_id'] ,
			# Convert coordinate system
			"point" : transform(inProj,outProj,val['stop_lon'],val['stop_lat'])
		
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
	with gzip.open("../out/gtfs_node_data.json.gz",'w') as outfile:
		json.dump(nodes_data,outfile,indent=2)

	print len(nodes_data), " nodes generated"

	print "Dumping address data"
	with gzip.open("../out/gtfs_address_data.json.gz",'w') as outfile:
		json.dump(address_data,outfile,indent=2)
