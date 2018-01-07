import gzip
import time
from pprint import pprint
import pandas as pd
import glob
import ujson as json

error_log = json.load(gzip.open("../out/error_log_.json.gz"))
print len(error_log), " errors detected"

results = []
for file in glob.glob("../out/gtfs_edge*"):
	print "Extracting from ", file
	data = json.load(gzip.open(file))
	results.extend(data)
		
print len(results), " results loaded"

pprint(results[100])