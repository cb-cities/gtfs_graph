import os
import sys
import json
import gzip
import glob
import igraph
from pprint import pprint
from shapely.geometry import Point, Polygon, LineString

def compute_average_jt(services):
	
	jt_total = 0
	for service in services:
		jt_total = jt_total + service['journey_time']

	average_jt = jt_total / float(len(services))

	return average_jt

def filter_modes(link):

	if "Subway, Metro" in link['modes']:
		return "Subway, Metro"
	elif "Tram, Streetcar, Light rail" in link['modes']:
		return "Tram, Streetcar, Light rail"
	elif "Ferry" in link['modes']:
		return "Ferry"
	else:
		return "Bus"


def compute_page_rank(nodes,links,pagerank_weight,addresses_db,inputted_mode):

	gtfs_graph = igraph.Graph.DictList(vertices=nodes,edges=links, vertex_name_attr="toid",edge_foreign_keys=('negativeNode',"positiveNode"),directed=True)

	print "Computing PageRank for inputted_mode ", inputted_mode

	pagerank_values = gtfs_graph.pagerank(weights=pagerank_weight,directed=True)

	print len(pagerank_values), "pagerank results"

	print "Curating PageRank results"

	vertex_list = []
	for v in gtfs_graph.vs:
		vertex_list.append(v)

	output = []
	for i in range(0,len(pagerank_values)):
		data = {
			"toid" : vertex_list[i]['toid'],
			"name" : addresses_db[vertex_list[i]['toid']]['text'],
			"PageRank_%" : pagerank_values[i] * 100
		}
		output.append(data)

	print "Sorting PageRank results"
	pagerank_output = sorted(output, key=lambda x:x['PageRank_%'],reverse=True)

	print "Creating a new graph with vertex pagerank vertex attributes"
	selected_toids = []
	pagerank_data_db = {}
	for i in range(0,len(pagerank_output)):
		pagerank_data_db[pagerank_output[i]['toid']] = pagerank_output[i]
		if i <= 300:
			selected_toids.append(pagerank_output[i]['toid'])

	for node in nodes:
		node['pagerank'] = pagerank_data_db[node['toid']]['PageRank_%'] * 3000

	print "Rebuild graph with pagerank as a vertex attribute"
	gtfs_graph = igraph.Graph.DictList(vertices=nodes,edges=links, vertex_name_attr="toid",edge_foreign_keys=('negativeNode',"positiveNode"),directed=True)

	coordinates = []
	lats = []
	lons = []
	for vertex in gtfs_graph.vs:
		if vertex['toid'] in selected_toids:
			vertex['label'] = addresses_db[vertex['toid']]['text']
		else:
			vertex['label'] = ""
		coords = (vertex['point'][0],vertex['point'][1])
		lats.append(vertex['point'][0])
		lons.append(vertex['point'][1])
		coordinates.append(coords)

	max_lat = sorted(lats)[-1]
	min_lat = sorted(lats)[0]
	
	max_lon = sorted(lons)[-1]
	min_lon = sorted(lons)[0]

	plot_y_dimensions = (max_lat - min_lat) * 10000
	plot_x_dimensions = (max_lon - min_lon) * 10000

	shape = (plot_y_dimensions,plot_x_dimensions)

	print gtfs_graph.summary()

	# Let's use the coordinates for the actual graph, spatially correct
	layout = coordinates
	# layout = gtfs_graph.layout_lgl()

	modes_color_dict = {
		"Bus": "green",
		"Subway, Metro" : "red",
		"Tram, Streetcar, Light rail" : "red",
		"Ferry" : "light blue"
		}

	visual_style = {}
	# vertex size = pagerank value
	visual_style['vertex_size'] = [pagerank for pagerank in gtfs_graph.vs['pagerank']]
	visual_style['vertex_color'] = "orange"
	visual_style['vertex_label_size'] = 20
	visual_style['layout'] = layout
	visual_style['vertex_label_angle'] = 1.5708
	visual_style['margin'] = 0
	visual_style['bbox'] = shape
	visual_style['rescale'] = False
	
	# add edge color equal to dominant mode
	visual_style['edge_color'] = [modes_color_dict[mode] for mode in gtfs_graph.es['mode']]

	if inputted_mode == "nope":

		file_name = "../out/pagerank_stats/plots/pagerank_graph_visualisation.pdf"
		print "Writing image ", file_name
		igraph.plot(gtfs_graph,file_name,**visual_style)

		print "Dumping raw files"
		with gzip.open("../out/pagerank_stats/data/pagerank_all.json.gz",'w') as outfile:
			json.dump(pagerank_output,outfile,indent=2)

	else:

		file_name = "../out/pagerank_stats/plots/pagerank_graph_visualisation_" + mode + ".pdf"
		print "Writing image ", file_name
		# igraph.plot(gtfs_graph,file_name,**visual_style)

		print "Dumping raw files"
		with gzip.open("../out/pagerank_stats/data/pagerank_"+mode+".json.gz",'w') as outfile:
			json.dump(pagerank_output,outfile,indent=2)

def check_dir(directory):

	if not os.path.exists(directory):
		os.makedirs(directory)

def extract_nodes(edges,nodes_db):
	
	nodes_toid_list = []
	for edge in edges:
		nodes_toid_list.append(edge['negativeNode'])
		nodes_toid_list.append(edge['positiveNode'])

	unique_nodes_toid_list = list(set(nodes_toid_list))

	tmp_nodes = []
	for toid in unique_nodes_toid_list:
		node = nodes_db[toid]
		tmp_nodes.append(node)

	return tmp_nodes

def filter_mode(record, mode):
	
	output = []
	for service in record['services']:
		if service['route_type'] == mode:
			output.append(service)
	
	if len(output) != 0:
		
		record.pop('services')
		record['services'] = output
		return True, record

	else:
		return False, 0

def filter_graph_modes(nodes, links, mode, nodes_db):

	print "extracting records for mode: ", mode

	tmp_edges = []
		
	for record in links:

		if len(record['modes']) == 1:
			if record['modes'][0] == mode:
				tmp_edges.append(record)
		
		else:
			
			match_status, matches = filter_mode(record,mode)

			if match_status == True:

				tmp_edges.append(matches)
	tmp_nodes = extract_nodes(tmp_edges, nodes_db)

	print len(tmp_edges), " edges loaded"
	print len(tmp_nodes), " nodes loaded"

	# print "Dumping nodes"
	# with gzip.open("../out/sierra-charlie/"+mode+"/nodes1.json.gz",'w') as outfile:
	# 	json.dump(data,outfile,indent=2)

	# print "Dumping links"
	# with gzip.open("../out/sierra-charlie/"+mode+"/links1.json.gz",'w') as outfile:
	# 	json.dump(tmp_edges,outfile,indent=2)

	return tmp_nodes, tmp_edges

macro_links = []
for file in glob.glob("../out/graph/*edges*.json.gz"):
	print "Loading ", file
	data = json.load(gzip.open(file))
	
	for link in data:
		
		link['no_services'] = len(link['services'])
		link['mode'] = filter_modes(link)

		# Get rid of all the service details as we won't use them for Pagerank compute
		link.pop("services")
		
		macro_links.append(link)

print len(macro_links), " links loaded"

# Load addresses
addresses = json.load(gzip.open("../out/graph/addresses_1.json.gz"))
print len(addresses), " addresses loaded"
tmp_nodes = json.load(gzip.open("../out/graph/nodes_1.json.gz"))
print len(tmp_nodes), " tmp nodes loaded"

# Somewhat arbitrary 
london_bounds = Polygon([[-0.7635498046875,51.09662294502995],[0.54931640625,51.09662294502995],[0.54931640625,51.839171715043946],[-0.7635498046875,51.839171715043946],[-0.7635498046875,51.09662294502995]])

nodes_db = {}
macro_nodes = []
for node in tmp_nodes:
	node_coords = Point(node['point'][1],node['point'][0])
	if node_coords.within(london_bounds):
		nodes_db[node['toid']] = node
		macro_nodes.append(node)

print len(macro_nodes), " nodes loaded for London area"

addresses_db = {}
for add in addresses:
	addresses_db[add['toid']] = add

compute_page_rank(macro_nodes, macro_links,"journey_time",addresses_db,"nope")

# all_modes_list = ['Bus', 'Subway, Metro', 'Tram, Streetcar, Light rail', 'Ferry']

# for inputted_mode in all_modes_list:

# 	print "Computing for mode", inputted_mode

# 	tmp_nodes, tmp_links = filter_graph_modes(macro_nodes,macro_links,inputted_mode,nodes_db)

# 	compute_page_rank(tmp_nodes,tmp_links,"journey_time",addresses_db,inputted_mode)

