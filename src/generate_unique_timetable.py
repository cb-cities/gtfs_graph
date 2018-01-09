import gzip
import time
from pprint import pprint
import pandas as pd
import glob
import ujson as json
from operator import itemgetter
import sys

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

print "Dumping to file"

chunkSize = 1000
for i in xrange(0, len(results), chunkSize):
	with gzip.open('../out/graph/unique_edge_' + str((i//chunkSize)+1) + '.json.gz', 'w') as outfile:
		json.dump(results[i:i+chunkSize], outfile, indent =2)

print "Finito"