import time
import json
import gzip
import glob
from ggplot import *
import pandas as pd
from pathlib import Path

def london_mode_converter(mode):

	# Because people don't like American names...
	if mode == "Subway, Metro":
	
		return "Underground"
	
	elif mode == "Tram, Streetcar, Light rail":
		
		return "Tram"

	else:
		return mode

def headway_extractor(edges):

	edge_headways = []
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
					"mode" : london_mode_converter(mode)
				}
				edge_headways.append(data)

	return edge_headways

def plot_headway_density(headways_df,plot_type, sample):

	file_name = "../out/plots/headway_density_" + plot_type + "_" + sample + ".png"

	print "plotting headway density plot"
		
	s = ggplot(aes(x="headway",color=plot_type),data=headways_df) + geom_density() + labs(x="Service headway (minutes)",y="Probability density function") + scale_y_continuous(limits=(0,0.6))

	s.save(file_name)

	print "Plotted: ", file_name

def day_to_day_name(day):
	
	if day in [0,1,2,3,4]:
		
		return "Weekday"

	else:

		return "Weekend"


pickled_file = "../out/graph/pickled_edges.pkl"
if Path(pickled_file).is_file():
	
	print "Pickeld versions exists, loading..."
	
	headways_df = pd.read_pickle(pickled_file)

	print len(headways_df), " records in headways dataframe"

else:

	print "No pickled file, so generating it..."

	# Density plot for service headways
	headways = []
	for file in glob.glob("../out/graph/*edge*.json.gz"):
		data = json.load(gzip.open(file))
		print "Extracting headways from ", file
		tmp_headways = headway_extractor(data)
		headways.extend(tmp_headways)

	# Convert to a pandas dataframe so we can use ggplot
	headways_df = pd.DataFrame(headways)

	print len(headways_df), " records in headways dataframe"

	print "Generating human readable time stamps"

	# Convert time stamp
	headways_df['day_of_week_int'] = pd.to_datetime(headways_df['time_stamp'], unit='s').dt.dayofweek

	# Add day of week
	headways_df['day_type'] = headways_df.apply(lambda row: day_to_day_name(row['day_of_week_int']), axis=1)

	# Add hour
	# Create hour to AM, PM and IP ftn
	# Classify AM, PM and IP

	headways_df.to_pickle(pickled_file)

# unique_day_types = headways_df.day_type.unique()
# for day in unique_day_types:
# 	print "plotting for day type: ", day
# 	tmp_df = headways_df[headways_df.day_type==day]
# 	print len(tmp_df), " records found"
# 	plot_headway_density(tmp_df, "mode", day)

plot_headway_density(headways_df,"mode","all")

print "Finished"
