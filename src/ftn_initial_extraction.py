import gzip
import time
from pprint import pprint
import pandas as pd
import glob
import ujson as json
from datetime import datetime, timedelta
import calendar
import sys
import ftn_extract_tar_gz
from pyproj import Proj, transform
import os

inProj = Proj(init='epsg:4326')
outProj = Proj(init='epsg:27700')

def check_if_gtfs_already_extracted(file):

	folder = file[:-7]

	if os.path.isdir(folder):
		
		print "GTFS already extracted"

	else:

		print "Extracting...."
		ftn_extract_tar_gz.extract(file)

	return folder + "/gtfs/"

def get_what_ya_need(path):

	print "Converting GTFS data into usable format"

	for file in glob.glob(path + "*"):
		file_name = file[35:-4]

		if file_name == "stops":
			print "Creating stops db"
			stops = pd.read_csv(file)
			stops_jd = json.loads(stops.to_json(orient='records'))
			stops_db ={}
			for stop in stops_jd:
				stops_db[stop['stop_id']] = stop
		
		elif file_name == "routes":
			print "Creating routes db"
			routes = pd.read_csv(file)
			routes_js = json.loads(routes.to_json(orient='records'))
			routes_db = {}
			for route in routes_js:
				routes_db[route['route_id']] = route

		elif file_name == "trips":
			print "Creating trips db"
			trips = pd.read_csv(file)
			trips_js = json.loads(trips.to_json(orient='records'))
			trips_db = {}
			for trip in trips_js:
				trips_db[trip['trip_id']] = trip
		
		elif file_name == "calendar":
			print "Creating calendar db"
			calendar = pd.read_csv(file)
			calendar_js = json.loads(calendar.to_json(orient='records'))
			calendar_db = {}
			for cal in calendar_js:
				calendar_db[cal['service_id']] = cal

		elif file_name == "stop_times":
			print "Creating stop_times db"
			stop_times_df = pd.read_csv(file)
			stop_times = json.loads(stop_times_df.to_json(orient='records'))
			print len(stop_times), " stop times loaded"
			stop_times_db = {}
			for stop in stop_times:
				stop_times_db[stop['trip_id']] = stop

	return trips_db, stops_db, routes_db, calendar_db, stop_times_db, stop_times

def timetable_day_over_run_ftn(day, stamp):

	# Add one day to day of week
	day = timedelta(days=1)
	new_time = "00" + stamp['departure_time'][2:]
	
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

	tic = time.time()

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
					
					except Exception as e:
						# If the service runs across midnight, you get incorrect time stamps like 24:01:00
						# Here, we add a day and manually fix the timestamp 
						new_time = timetable_day_over_run_ftn(day,current_stop)
						departure_time_dt = datetime.strptime(new_time,"%H:%M:%S").time()

					try:
						arrival_time_dt = datetime.strptime(current_stop['arrival_time'],"%H:%M:%S").time()
					
					except Exception as e:

						new_time = timetable_day_over_run_ftn(day,current_stop)
						arrival_time_dt = datetime.strptime(new_time,"%H:%M:%S").time()
					
					dep_gen_stamp = datetime.combine(day,departure_time_dt)
					arr_gen_stamp = datetime.combine(day,arrival_time_dt)

					# Create a dict. Format time stamps as epoch to get rid of this str/datetime object nonsense
					data = {
						
						"departure_time" : dep_gen_stamp.strftime('%s'),
						"arrival_time" : arr_gen_stamp.strftime('%s')
						
						}

					time_tabled_services.append(data)
			
			data = {
				'negativeNode' : neg_node,
				"positiveNode" : pos_node,
				"trip_id" : trip_id_1,
				"stop_sequence" : stop_sequence,
				"time_tabled_services" : time_tabled_services,
				"service_information" : route_data
			}

			current_trip.append(data)

		else:
			print "One trip extracted"

			# Append all the results to big list
			all_unique_trips.append(current_trip)
			
			# Reset sequence
			previous_step_sequence = 1
			
			# Reset list
			current_trip = []

	collated_outputs = []

	# Second sweep

	for unique_trip in range(len(all_unique_trips)):

		for i in range(0,len(all_unique_trips[unique_trip])-1):
			link_data = []
			stop_data = all_unique_trips[unique_trip][i]
			next_stop_data = all_unique_trips[unique_trip][i+1]

			for z in range(0,len(stop_data['time_tabled_services'])):
				dep_time = int(stop_data['time_tabled_services'][z]['departure_time'])
		
				arr_time = int(next_stop_data['time_tabled_services'][z]['arrival_time'])
				
				data = {
					"departure_time" : dep_time,
					"arrival_time" : arr_time,
					"journey_time" : (arr_time - dep_time)
				}
				
				link_data.append(data)
			
			all_unique_trips[unique_trip][i].pop("time_tabled_services")
			all_unique_trips[unique_trip][i]['services'] = link_data

	print "Dumping results to file"

	chunkSize = 10000
	for i in xrange(0, len(all_unique_trips), chunkSize):
		with gzip.open('../out/all_edges/gtfs_edge_data_' + str((i//chunkSize)+1) + '.json.gz', 'w') as outfile:
			json.dump(all_unique_trips[i:i+chunkSize], outfile, indent =2)

	print len(all_unique_trips), " trips generated"

	toc = time.time()

	time_per_unique_trip = (toc - tic) / len(all_unique_trips)

	print "Time per unique trip", time_per_unique_trip

def assess_gtfs_coverage(stop_times_db,trips_db,routes_db):
	
	# What trip ids in trips_db aren't mentioned in routes_db

	not_found = []
	
	trip_ids = trips_db.keys()
	for trip_id in trip_ids:
		try:
			works = stop_times_db[trip_id]
			print "found"
		except KeyError:
			route_missing = trips_db[trip_id]['route_id']
			not_found.append(route_missing)

	unique_missing = list(set(not_found))

	data = [{
	"missing_route_ids" : not_found
	}]

	print len(unique_missing), " missing"
	print len(routes_db), " in original list"

	with gzip.open("../out/missing_routes.json.gz",'w') as outfile:
		json.dump(data,outfile,indent=2)
