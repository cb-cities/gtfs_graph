import os
import sys
import json
import gzip
import glob
import igraph
from pprint import pprint

def compute_page_rank(nodes,links,pagerank_weight,addresses_db):

	gtfs_graph = igraph.Graph.DictList(vertices=nodes,edges=links, vertex_name_attr="toid",edge_foreign_keys=('negativeNode',"positiveNode"),directed=True)

	print "Computing PageRank"

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

	with gzip.open("../out/pagerank_stats/pagerank_all.json.gz",'w') as outfile:
		json.dump(pagerank_output,outfile,indent=2)

def compute_page_rank_per_mode(nodes,links,pagerank_weight,addresses_db,mode):

	gtfs_graph = igraph.Graph.DictList(vertices=nodes,edges=links, vertex_name_attr="toid",edge_foreign_keys=('negativeNode',"positiveNode"),directed=True)

	print gtfs_graph.summary() + " for mode :" + str(mode)

	print "Computing PageRank"

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

	with gzip.open("../out/pagerank_stats/" + mode + "/pagerank.json.gz",'w') as outfile:
		json.dump(pagerank_output,outfile,indent=2)

def compute_aggregated_page_rank(mode):

	overall_db = {}
	data = json.load(gzip.open("../out/pagerank_stats/pagerank_"+mode+".json.gz"))
	print str(len(data)) + " records, per mode " + mode
	for record in data:
		if record['toid'] not in overall_db:
			overall_db[record['toid']] = []
		overall_db[record['toid']].append(record)

	keys = overall_db.keys()
	values = overall_db.values()

	output = []
	for i in range(0,len(keys)):
		data = {
			"location" : addresses_db[keys[i]]['text'],
			"data" : values[i]
		}

		output.append(data)

	# Not sure this is possible - are they add'able?
	for record in output:

		total_page_rank = 0
		for value in record['data']:
			total_page_rank = total_page_rank + value['PageRank_%']
		record.pop("data")

		record['total_page_rank'] = total_page_rank

	sorted_output = sorted(output, key=lambda x:x['total_page_rank'],reverse=True)

	data = {
		"mode" : "mode",
		"data" : sorted_output
	}

	return data

def check_dir(directory):

	if not os.path.exists(directory):
		os.makedirs(directory)

# below for extracting sub graphs

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

		sample_record = record
		
		match_status, matches = filter_mode(sample_record,mode)

		if match_status == True:

			tmp_edges.append(matches)

		# Just for sierra-charlie
		# for record in tmp_edges:
		# 	record['toid'] = record['edge_id']
		# 	record.pop("edge_id")

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

links = []
for file in glob.glob("../out/graph/*edges*"):
	print "Loading ", file
	data = json.load(gzip.open(file))
	for link in data:
		link['no_services'] = len(link['services'])
		# link.pop("services")
		links.append(link)

print len(links), " links loaded"

# Load addresses
addresses = json.load(gzip.open("../out/graph/addresses_1.json.gz"))
print len(addresses), " addresses loaded"
nodes = json.load(gzip.open("../out/graph/nodes_1.json.gz"))
print len(nodes), " nodes loaded"

nodes_db = {}
for node in nodes:
	nodes_db[node['toid']] = node

addresses_db = {}
for add in addresses:
	addresses_db[add['toid']] = add

# compute_page_rank(nodes, links,"no_services",addresses_db)

all_modes_list = [u'Subway, Metro', u'Bus', u'Tram, Streetcar, Light rail', u'Ferry']

# ~10mins run time
for mode in all_modes_list:

	print "Computing for mode", mode
	
	try:
		
		print "Found existing results, loading from file"

		pagerank_data = json.load(gzip.open("../out/pagerank_stats/"+mode+"/pagerank.json.gz"))
		pagerank_data_db = {}
		for record in pagerank_data:
			pagerank_data_db[record['toid']] = record

		tmp_nodes, tmp_links = filter_graph_modes(nodes,links,mode,nodes_db)
		for tmp_node in tmp_nodes:
			tmp_node['pagerank'] = pagerank_data_db[tmp_node['toid']]['PageRank_%']

		local_graph = igraph.Graph.DictList(vertices=tmp_nodes,edges=tmp_links, vertex_name_attr="toid",edge_foreign_keys=('negativeNode',"positiveNode"),directed=True)

		coordinates = []
		for vertex in local_graph.vs:
			vertex['label'] = addresses_db[vertex['toid']]['text']
			coords = (vertex['point'][0],vertex['point'][1])
			coordinates.append(coords)
		
		print local_graph.summary()

		# Let's use the coordinates for the actual layour
		layout = coordinates

		file_name = "../out/pagerank_stats/" + mode + "/pagerank_vis.pdf"

		igraph.plot(local_graph,file_name,layout=layout)

	except IOError:

		print "Result not found, computing"
		check_dir("../out/pagerank_stats/" + mode)
		
		compute_page_rank_per_mode(tmp_nodes,tmp_links,"journey_time",addresses_db,mode)

# metrics = []
# for mode in all_modes_list:
# 	data = compute_aggregated_page_rank(mode)
# 	metrics.append(data)

# with gzip.open("../out/pagerank_stats/pagerank_stats_all_modes.json.gz","w") as outfile:
# 	json.dump(metrics,outfile,indent=2)
