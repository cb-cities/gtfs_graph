import gzip
import time
from pprint import pprint
import pandas as pd
import tarfile
import glob
import ujson as json
from datetime import datetime, timedelta
import calendar
import sys
from pyproj import Proj, transform
import os

inProj = Proj(init='epsg:4326')
outProj = Proj(init='epsg:27700')

def extract(file_name):

	print "Working on ", file_name
	dest_folder = "../tmp/"
	
	tar = tarfile.open(file_name, "r:gz")
	tar.extractall(dest_folder)
	
	tar.close()

def check_if_gtfs_already_extracted(file):

	folder = file[:-7]

	if os.path.isdir(folder):
		
		print "GTFS already extracted"

	else:

		print "Extracting...."
		extract(file)

	return folder + "/gtfs/"

def get_what_ya_need(path):

	print "Converting GTFS data into usable format"

	for file in glob.glob(path + "*"):

		if "stops" in file:
			print "Creating stops db"
			stops = pd.read_csv(file)
			stops_jd = json.loads(stops.to_json(orient='records'))
			stops_db ={}
			for stop in stops_jd:
				stops_db[stop['stop_id']] = stop
		
		elif "routes" in file:
			print "Creating routes db"
			routes = pd.read_csv(file)
			routes_js = json.loads(routes.to_json(orient='records'))
			routes_db = {}
			for route in routes_js:
				routes_db[route['route_id']] = route

		elif "trips" in file:
			print "Creating trips db"
			trips = pd.read_csv(file)
			trips_js = json.loads(trips.to_json(orient='records'))
			trips_db = {}
			for trip in trips_js:
				trips_db[trip['trip_id']] = trip
		
		elif "calendar" in file:
			print "Creating calendar db"
			calendar = pd.read_csv(file)
			calendar_js = json.loads(calendar.to_json(orient='records'))
			calendar_db = {}
			for cal in calendar_js:
				calendar_db[cal['service_id']] = cal

		elif "stop_times" in file:
			print "Creating stop_times db"
			stop_times_df = pd.read_csv(file)
			stop_times = json.loads(stop_times_df.to_json(orient='records'))
			print len(stop_times), " stop times loaded"
			stop_times_db = {}
			for stop in stop_times:
				stop_times_db[stop['trip_id']] = stop

	return trips_db, stops_db, routes_db, calendar_db, stop_times_db, stop_times

def generate_unique_edges(results):

	print "Generating unique edge files"

	print len(results), " edges loaded"

	print "Generating unique edge id"
	edge_ids = []
	for result in results:
		for data in result:
			edge_id = data['negativeNode'] + data['positiveNode']
			data['service_id'] = edge_id
			edge_ids.append(edge_id)

	unique_edges = list(set(edge_ids))

	print "Flatting edges list"

	results_flat = [item for sublist in results for item in sublist]

	print "Sorting edges list by service_id"

	sorted_list = sorted(results_flat, key=lambda k: k['service_id'])

	print "Now extracting unique edges"

	tic = time.time()
	output = {}
	for record in sorted_list:
		key = record['service_id']
		if key not in output:
			output[key] = []
		output[key].append(record)
	toc = time.time()

	print str(toc - tic) + " time elapsed for " + str(len(results)) + " records"

	output_keys = output.keys()
	output_values = output.values()

	print "Creating a nice JSON object out of the results"

	results = []
	for i in range(0,len(output_keys)):
		data = {
			"service_id" : output_keys[i],
			"records" : output_values[i]
		}
		results.append(data)

	print len(results), " unique edges found"

	return results

def timetable_day_over_run_ftn(stamp):

	# this is really bloody hacky... you can't use python datetime lib as it freaks out at non 24hr days...
	# take first 2 charactes, turn to int, take away 24
	new_hr = str(int(stamp['departure_time'][:2]) - 24)

	if len(new_hr) == 1:
		new_hr = "0" + new_hr 

	new_time = (str(new_hr)+stamp['departure_time'][2:])
	
	return new_time

def route_type_dict(route_data):
	
	type_dict = {
		0 : {
			"route_type" : 0,
			"route_type_desc" : "Tram, Streetcar, Light rail"
			},
		1 : {
			"route_type" : 1,
			"route_type_desc" : "Subway, Metro"
			},
		2 : {
			"route_type" : 2,
			"route_type_desc" : "Rail"
			},
		3 : {
			"route_type" : 3,
			"route_type_desc" : "Bus"
			},
		4 : {
			"route_type" : 4,
			"route_type_desc" : "Ferry"
			},
		5 : {
			"route_type" : 5,
			"route_type_desc" : "Cable car"
			},
		6 : {
			"route_type" : 6,
			"route_type_desc" : "Gondola, Suspended cable car"
			},
		7 : {
			"route_type" : 7,
			"route_type_desc" : "Funicular"
			}
		}

	queried_type = type_dict[route_data]['route_type_desc']

	return queried_type

def create_edges_with_timetable_info(trips_db, stops_db, routes_db, calendar_db, stop_times_db, stop_times):

	print "Creating edges for services"

	all_unique_trips = []
	current_trip = []
	previous_step_sequence = 1

	print len(stop_times), " stop_times loaded"

	for i in range(0,len(stop_times)-1):
		
		current_stop = stop_times[i]
		trip_id_ = current_stop['trip_id']
		route_id = trips_db[trip_id_]['route_id']
		route_data = routes_db[str(route_id)]

		route_data = {
			"agency" : route_data['agency_id'],
			"route_short_name" : route_data['route_short_name'],
			"route_id" : route_data['route_id'],
			"route_long_name" : route_data['route_long_name'],
			"route_type" : route_type_dict(route_data['route_type'])
		}

		next_stop = stop_times[i+1]

		if next_stop['stop_sequence'] > previous_step_sequence:

			if current_stop['stop_sequence'] == 1:
				start_of_route = True
			else:
				start_of_route = False
			
			previous_step_sequence = next_stop['stop_sequence']
			
			neg_node = current_stop['stop_id']
			pos_node = next_stop['stop_id']
			
			trip_id_1 = current_stop['trip_id']
			trip_id_2 = next_stop['trip_id']

			stop_sequence = str(current_stop['stop_sequence']) + "_to_" + str(next_stop['stop_sequence'])
			if trip_id_1 != trip_id_2:
				print "Logic error"
				sys.exit(1)

			# Get service_id from trips_db
			service_id = trips_db[trip_id_1]['service_id']
			
			# Get calendar info from service_id
			date_info = datetime.strptime(str(calendar_db[service_id]['start_date']),'%Y%m%d')

			# Let's find first Monday from the timetable
			time_delta = 7 - int(date_info.weekday())
			start_of_week = date_info + timedelta(days=time_delta)
			
			# Find end of week
			end_of_week = start_of_week + timedelta(days=6)

			service_days_info = calendar_db[service_id]

			time_tabled_services = []
			
			# Iterate over the week
			for i in range(0,7):
				
				day = start_of_week + timedelta(days=i)
				day_of_week = calendar.day_name[day.weekday()]
				
				if service_days_info[day_of_week.lower()] == 1 or service_days_info[day_of_week.lower()] == 1.0 :
					
					# Append date to this time to create a datetime object
					try:
						departure_time_dt = datetime.strptime(current_stop['departure_time'],"%H:%M:%S").time()
						dep_day = day
					
					except Exception as e:
						# If the service runs across midnight, you get incorrect time stamps like 24:01:00
						# Here, we add a day and manually fix the timestamp
						new_time = timetable_day_over_run_ftn(current_stop)
						departure_time_dt = datetime.strptime(new_time,"%H:%M:%S").time()
						# Add the day
						dep_day = day + timedelta(1)

					try:
						arrival_time_dt = datetime.strptime(next_stop['arrival_time'],"%H:%M:%S").time()
						arr_day = day
					
					except Exception as e:

						# If the service runs across midnight, you get incorrect time stamps like 24:01:00
						# Here, we add a day and manually fix the timestamp
						new_time = timetable_day_over_run_ftn(next_stop)
						arrival_time_dt = datetime.strptime(new_time,"%H:%M:%S").time()
						# Add the day
						arr_day = day + timedelta(1)
					
					# Convert and format time stamps as epoch to get rid of this str/datetime object nonsense
					dep_gen_stamp = int((datetime.combine(dep_day,departure_time_dt)).strftime('%s'))
					arr_gen_stamp = int((datetime.combine(arr_day,arrival_time_dt)).strftime('%s'))
					journey_time = arr_gen_stamp - dep_gen_stamp

					if journey_time < 0:

						print "day", day
						
						print "negative Node"
						pprint(current_stop)
						print "positive Node"
						pprint(next_stop)

						print "journey time ", journey_time
						sys.exit(1)

					# Create a dict of ze results

					data = {
						
						"departure_time" : dep_gen_stamp,
						"arrival_time" : arr_gen_stamp,
						'journey_time' : journey_time
						
						}

					time_tabled_services.append(data)
			
			data = {
				'negativeNode' : neg_node,
				"positiveNode" : pos_node,
				"trip_id" : trip_id_1,
				"stop_sequence" : stop_sequence,
				"services" : time_tabled_services,
				"service_information" : route_data
			}

			current_trip.append(data)

		else:

			# print "One trip extracted"

			# Append all the results to big list
			all_unique_trips.append(current_trip)
			
			# Reset sequence
			previous_step_sequence = 1
			
			# Reset list
			current_trip = []

	return all_unique_trips

def assess_gtfs_coverage(stop_times_db,trips_db,routes_db):
	
	# Find what trip ids in trips_db aren't mentioned in routes_db

	not_found = []
	
	trip_ids = trips_db.keys()
	for trip_id in trip_ids:
		try:
			works = stop_times_db[trip_id]
		except KeyError:
			route_missing = trips_db[trip_id]['route_id']
			print "Route ", route_missing, " not found in routes_db"
			not_found.append(route_missing)

	unique_missing = list(set(not_found))

	data = [{
		
		"missing_route_ids" : not_found
	
	}]

	print "There are " + str(len(unique_missing)) + " missing records from the original " + str(len(routes_db))

	print "Dumping to file (../out/data_errors/missing_routes.json.gz)"

	with gzip.open("../out/data_errors/missing_routes.json.gz",'w') as outfile:
		json.dump(data,outfile,indent=2)
