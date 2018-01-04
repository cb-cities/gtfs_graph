import gzip
import time
from pprint import pprint
import pandas as pd
import glob
import ujson as json

results = []
for file in glob.glob("../out/gtfs_edge*14*"):
	print "Extracting from ", file
	data = json.load(gzip.open(file))
	results.extend(data)
	for record in data:
		id_ = record['']
		
print len(results), " results loaded"

pprint(results[0])
