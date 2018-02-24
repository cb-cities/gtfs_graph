import ftn_initial_extraction
import ftn_generate_weighted_graph

input_file = "../tmp/tfl_gtfs_2017-02-07_05.tar.gz"

# Initial extraction into graph'esque data
extracted_folder = ftn_initial_extraction.check_if_gtfs_already_extracted(input_file)

# A series of dicts with GTFS data in a useful format
trips_db, stops_db, routes_db, calendar_db, stop_times_db, stop_times = ftn_initial_extraction.get_what_ya_need(extracted_folder)

# Check the GTFS data itself
# no point during testing....
ftn_initial_extraction.assess_gtfs_coverage(stop_times_db,trips_db,routes_db)

# Create edges per service
all_gtfs_edges = ftn_initial_extraction.create_edges_with_timetable_info(trips_db, stops_db, routes_db, calendar_db, stop_times_db, stop_times)

# Create unique edges
unique_gtfs_edges = ftn_initial_extraction.generate_unique_edges(all_gtfs_edges)

# Create a weighted graph, based upon average journey times on an edge
edges = ftn_generate_weighted_graph.gen_edges(unique_gtfs_edges,stops_db)
# Create the nodes and addresses file
nodes, addresses = ftn_generate_weighted_graph.gen_nodes(stops_db)

# Dump results to file
ftn_generate_weighted_graph.file_dumper(edges,"edges_", 1000)
ftn_generate_weighted_graph.file_dumper(nodes,"nodes_", 200000)
ftn_generate_weighted_graph.file_dumper(addresses, "addresses_", 200000)
