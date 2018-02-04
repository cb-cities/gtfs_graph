import ftn_initial_extraction
import ftn_generate_unique_edges
import ftn_generate_weighted_graph

# Part A
input_file = "../tmp/tfl_gtfs_2017-02-07_05.tar.gz"

# Initial extraction into graph'esque data
extracted_folder = ftn_initial_extraction.check_if_gtfs_already_extracted(input_file)

trips_db, stops_db, routes_db, calendar_db, stop_times_db, stop_times = ftn_initial_extraction.get_what_ya_need(extracted_folder)

# ftn_initial_extraction.create_edges_with_timetable_info(trips_db, stops_db, routes_db, calendar_db, stop_times_db, stop_times)

# ftn_initial_extraction.assess_gtfs_coverage(routes_db,trips_db,routes_db)

# B

# Generate unique edges
# ftn_generate_unique_edges.generate_unique_edges("../out/gtfs_edge*")

# print "Starting to generate node data...."
# ftn_generate_weighted_graph.create_nodes(stops_db)

# gen an average weighted graph (for all records)
ftn_generate_weighted_graph.gen_frequency_based_weighted_graph(stops_db)