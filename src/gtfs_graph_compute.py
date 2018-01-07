import gzip
import time
from pprint import pprint
import pandas as pd
import glob
import ujson as json

# results = []
# for file in glob.glob("../out/gtfs_edge*11*"):
# 	print "Extracting from ", file
# 	data = json.load(gzip.open(file))
# 	results.extend(data)
# 	# for record in data:
		
# print len(results), " results loaded"

# pprint(results[26])

error_log = json.load(gzip.open("../tmp_out/error_log_.json.gz"))
print len(error_log)

data = []
for record in error_log:
	exception = record['exception']
	# status = record['data']
	data.append(exception)

data_un = list(set(data))

pprint(data_un)

# alt_log = json.load(gzip.open("../out/error_log_.json.gz"))
# print len(alt_log)

# data = []
# for record in alt_log:
# 	status = record['data']
# 	data.append(status)

# data_un = list(set(data))

# pprint(data_un)

