import sys
import json
import gzip
import glob
import igraph
from pprint import pprint

nodes = json.load(gzip.open("../out/graph/gtfs_node_data.json.gz"))
print len(nodes), " nodes loaded"
addresses = json.load(gzip.open("../out/graph/gtfs_address_data.json.gz"))
print len(addresses), " addresses loaded"

addresses_db = {}
for add in addresses:
	addresses_db[add['toid']] = add

edges = []
for file in glob.glob("../out/graph/gtfs*edge*4*.json.gz"):
	data = json.load(gzip.open(file))
	
	for record in data:
		record['number_services'] = len(record['services'])

	print "loading ", file
	edges.extend(data)

pprint(edges[10])

sys.exit(1)

test = sorted(edges, key=lambda x:x['number_services'])

pprint(test[-10:-1])

sys.exit(1)

print len(edges), " edges loaded"

gtfs_graph = igraph.Graph.DictList(vertices=nodes,edges=edges, vertex_name_attr="toid",edge_foreign_keys=('negativeNode',"positiveNode"),directed=True)

print "Computing PageRank"

pagerank_values = gtfs_graph.pagerank(weights="number_services",directed=True)

print "Curating PageRank results"

vertex_list = []
for v in gtfs_graph.vs:
	vertex_list.append(v['toid'])

output = []
for i in range(0,len(pagerank_values)):
	data = {
		"toid" : vertex_list[i],
		"name" : addresses_db[vertex_list[i]]['text'],
		"PageRank" : pagerank_values[i]
	}
	output.append(data)

print "Sorting PageRank results"
pagerank_output = sorted(output, key=lambda x:x['PageRank'])

print "Dumping to file"

with gzip.open("../out/summary/pagerank.json.gz",'w') as outfile:
	json.dump(pagerank_output,outfile,indent=2)

print "Finished"