import gzip
import time
from pprint import pprint
import pandas as pd
import glob
import ujson as json
import sys
import pandas as pd
# from ggplot import *

unique_edges = []
for file in glob.glob("../out/unique*.json.gz"):
	print "Working on: ", file
	data = json.load(gzip.open(file))
	unique_edges.extend(data)

print len(unique_edges), " records loaded"

types = []
for unique_edge in unique_edges:
	for record in unique_edge:
		type_ = record['service_information']['route_type']
		types.append(type_)

types = list(set(types))

# unique_edges = unique_edges[0:10]

edges = []
for unique_edge in unique_edges:
	
	data = {
		"negativeNode" : unique_edge[0]['negativeNode'],
		"positiveNode" : unique_edge[0]['positiveNode'],
		"gen_edge_id" : unique_edge[0]['service_id']
		}
	
	services = []
	
	for record in unique_edge:
		
		for service in record['services']:

			try:
		
				service_data = {
					"service_type" : record['service_information']['route_type'],
					"service_agency" : record['service_information']['agency'],
					"service_route_id" : record['service_information']['route_id'],
					"arrival_time" : service['arrival_time'],
					"departure_time" : service['departure_time'],
					"journey_time" : service['journey_time']
					}

				services.append(service_data)

			except Exception as e:
				x = 1
				
			# 	# service['departure_time'] = service['arrival_time']
				
			# 	service_data = {
			# 		"service_type" : record['service_information']['route_type'],
			# 		"service_agency" : record['service_information']['agency'],
			# 		"service_route_id" : record['service_information']['route_id'],
			# 		"arrival_time" : service['arrival_time'],
			# 		"departure_time" : service['departure_time'],
			# 		"journey_time" : service['journey_time']
			# 		}

			# 	services.append(service_data)
				
	# Order services by departure_time
	ordered_services = sorted(services, key=lambda k: k['departure_time'])

	data['services'] = ordered_services

	edges.append(data)

headways = []
for edge in edges:
	services = edge['services']
	services = sorted(services, key=lambda k: k['departure_time'])
	for i in range(0,len(services)-1):
		
		# Test - Only compare the same service
		if services[i+1]['service_route_id'] == services[i]['service_route_id']:
			
			headway = int(services[i+1]['departure_time']) - int(services[i]['departure_time'])
			
			if headway < 0:
				print headway
				pprint(services[i+1])
				pprint(services[i])
				sys.exit(1)
		
			# 10hrs
			if headway < 2000:
				time_stamp = services[i]['departure_time']
				mode = services[i]['service_type']
				
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

# chunkSize = 10000
# for i in xrange(0, len(edges), chunkSize):
# 	with gzip.open('../out/graph/edge_' + str((i//chunkSize)+1) + '.json.gz', 'w') as outfile:
# 		json.dump(edges[i:i+chunkSize], outfile, indent =2)