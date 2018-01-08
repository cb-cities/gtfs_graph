import glob
import json
import gzip
import tarfile
from pprint import pprint
import pandas as pd
import sys
from pyproj import Proj, transform

inProj = Proj(init='epsg:4326')
outProj = Proj(init='epsg:27700')

def extract(file_name):

	print "Extracting...."
	tar = tarfile.open(file_name, "r:gz")
	dest_folder = "../tmp/" + file_name[6:-7]
	tar.extractall(dest_folder)
	tar.close()

	print "Now available in", dest_folder