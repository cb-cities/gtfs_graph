import gzip
import time
from pprint import pprint
import pandas as pd
import glob
import ujson as json
from operator import itemgetter

error_log = json.load(gzip.open("../out/error_log_.json.gz"))
print len(error_log), " errors detected"

results = []
for file in glob.glob("../out/gtfs_edge*"):
	print "Extracting from ", file
	data = json.load(gzip.open(file))
	results.extend(data)
		
print len(results), " results loaded"

print "Adding unique edge ids"
edge_ids = []
for result in results:
	for data in result:
		edge_id = data['negativeNode'] + data['positiveNode']
		data['service_id'] = edge_id
		edge_ids.append(edge_id)

unique_edges = list(set(edge_ids))

print "Flatting results list"

results_flat = [item for sublist in results for item in sublist]

sorted_list = sorted(results_flat, key=lambda k: k['service_id'])

print "Now extracting unique edges"

tic = time.time()
output = []
for unique_edge in unique_edges:
	print "Looking for", unique_edge
	unique_edge_data = []
	for i in range(0,len(sorted_list)):
		if sorted_list[i]['service_id'] == unique_edge:
			unique_edge_data.append(sorted_list[i])

	print len(unique_edge_data), " records found"

	output.append(unique_edge_data)

toc = time.time()

print toc - tic

print "Dumping to file"

chunkSize = 10000
for i in xrange(0, len(output), chunkSize):
	with gzip.open('../out/graph/edge_' + str((i//chunkSize)+1) + '.json.gz', 'w') as outfile:
		json.dump(output[i:i+chunkSize], outfile, indent =2)
