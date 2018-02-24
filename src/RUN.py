import ftn_initial_extraction
import ftn_generate_weighted_graph

input_file = "../tmp/tfl_gtfs_2017-02-07_05.tar.gz"

# Initial extraction into graph'esque data
extracted_folder = ftn_initial_extraction.check_if_gtfs_already_extracted(input_file)

trips_db, stops_db, routes_db, calendar_db, stop_times_db, stop_times = ftn_initial_extraction.get_what_ya_need(extracted_folder)

# Check of the GTFS data itself

ftn_initial_extraction.assess_gtfs_coverage(stop_times_db,trips_db,routes_db)

all_gtfs_edges = ftn_initial_extraction.create_edges_with_timetable_info(trips_db, stops_db, routes_db, calendar_db, stop_times_db, stop_times)

unique_gtfs_edges = ftn_initial_extraction.generate_unique_edges(all_gtfs_edges)

ftn_generate_weighted_graph.gen_edges(unique_gtfs_edges,stops_db)

# # print "Starting to generate node data...."
# # ftn_generate_weighted_graph.create_nodes(stops_db)