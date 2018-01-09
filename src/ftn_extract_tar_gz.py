import glob
import json
import gzip
import tarfile

def extract(file_name):

	print "Working on ", file_name
	dest_folder = "../tmp/"
	
	tar = tarfile.open(file_name, "r:gz")
	tar.extractall(dest_folder)
	
	tar.close()