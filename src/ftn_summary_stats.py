import json
import gzip
import glob
from ggplot import *
import pandas as pd

def headway_calculator(edges):

	print "plotting headway density plot for " + str(len(edges)) + " edges"

	headways = []
	for edge in edges:
		
		# Extract service times
		services = edge['services']

		# Order services by departure_time (again, as we may have multiple operators, and routes)
		ordered_services = sorted(services, key=lambda k: k['departure_time'])
		
		for i in range(0,len(ordered_services)-1):
				
			# Calculate headway (in minutes)
			headway = (int(ordered_services[i+1]['departure_time']) - int(ordered_services[i]['departure_time'])) / 60.0
			
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

	file_name = "../out/plots/headway_density.png"
	
	s = ggplot(aes(x="headway",color="mode"),data=headways_df) + geom_density() + labs(x="Service headway (minutes)",y="Probability density function")
	
	s.save(file_name)

	print "Plotted: ", file_name

# Density plot for service headways
edges = []
for file in glob.glob("../out/graph/*edge*.json.gz"):
	data = json.load(gzip.open(file))
	edges.extend(data)

headway_calculator(edges)