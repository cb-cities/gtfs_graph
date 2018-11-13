import argparse


import ftn_initial_extraction
import ftn_generate_weighted_graph

if __name__ == '__main__':

	arg_parser = argparse.ArgumentParser(description='Convert GTFS to a DAG')

	arg_parser.add_argument('-i',
							'--input',
							help='The input file location',
							required=True)
	arg_parser.add_argument('-w',
							'--weights',
							help='If True, weights (actual services) are added as a list',
							required=False,
							default=False)


	args = vars(arg_parser.parse_args())

	input_file = args['input']
	keep_services = args['weights']

	if keep_services == False:
		print("Services not appended to edges. Computing average edge weight")

	print("Loading from folder {} ").format(input_file)

	# A series of dicts with GTFS data in a useful format
	trips_db, stops_db, routes_db, calendar_db, stop_times_db, stop_times = ftn_initial_extraction.get_what_ya_need(input_file)

	# # Check the GTFS data itself
	ftn_initial_extraction.assess_gtfs_coverage(stop_times_db,trips_db,routes_db)

	# # Create edges per service
	all_gtfs_edges = ftn_initial_extraction.create_edges_with_timetable_info(trips_db, stops_db, routes_db, calendar_db, stop_times_db, stop_times)

	# # Create unique edges
	unique_gtfs_edges = ftn_initial_extraction.generate_unique_edges(all_gtfs_edges)

	# # Create a weighted graph, based upon average journey times on an edge
	edges = ftn_generate_weighted_graph.gen_edges(unique_gtfs_edges,stops_db,keep_services)
	
	# Create the nodes and addresses file
	nodes, addresses = ftn_generate_weighted_graph.gen_nodes(stops_db)

	# Dump results to file
	
	ftn_generate_weighted_graph.file_dumper(edges,"edges_", 1000)
	ftn_generate_weighted_graph.file_dumper(nodes,"nodes_", 200000)
	ftn_generate_weighted_graph.file_dumper(addresses, "addresses_", 200000)
