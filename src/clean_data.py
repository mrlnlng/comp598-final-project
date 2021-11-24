"""
Script that parses the data collected and checks for:
	- Tweet uniqueness
	- Language: English
	- High likelihood of it being related to the movie

This script is specific about "Shang-Chi and the Ten Rings"
movie.

Authors: Teresa Altamirano Mayoral, Parsa Yadollahi
"""
#*********************************Imports*********************************
import sys, os
import pandas as pd
import argparse
import json
import math
import networkx as nx
import numpy as np


#*********************************Functions*********************************
"""
Function that reads the arguments with flags -i and -o
"""
def read_arguments():
	parser = argparse.ArgumentParser()
	parser.add_argument("-i", "--input", help="Absolute path of the input CSV file")
	parser.add_argument("-o", "--output", help="Absolut path of the output JSON file")

	return parser.parse_args()
"""
Function that return the absolute path of the file (fpath) and the absolute path of the
folder (folder).

This is in case the folder has to be created later on.
"""
def get_absfpath(input_fpath, store_location=''):
	curr_dir = os.path.dirname(os.path.abspath(__file__))
	folders = input_fpath.split('/')
	folder = os.path.join(curr_dir, store_location)
	for i in range(0, len(folders)-1):
		folder = os.path.join(folder, folders[i])

	fpath = os.path.join(folder, folders[-1])
	return fpath, folder

"""
Function creates the output folder (if necessary) and the JSON file 
such that every line is a new JSON object.

Takes Dictionary, String, String, Boolean, Boolean
"""
def create_output(output_dict, fpath, folder="", overwrite=True, indent=True):
	if(overwrite):
		mode = 'w'
	else:
		mode = 'a'
	try:
		with open(fpath, mode) as f:
			if(indent):
				json.dump(output_dict, f, indent=2)
			else:
				json.dump(output_dict, f)

			f.write('\n')
	except: 
		# print(path)
		os.mkdir(folder)
		with open(fpath, mode) as f:
			if(indent):
				json.dump(output_dict, f, indent=2)
			else:
				json.dump(output_dict, f)
			f.write('\n')

"""
Function reads a CSV given an absolute path and returns 
a Data Frame

Takes String
Returns DataFrame
"""
def read_csv(fpath):
	df = pd.read_csv(fpath)

	return df

"""
Function that goes from JSON file to dictionary.

Takes String (absolute path of JSON file)
Returns Dictionary
"""
def json_to_dict(fpath):
	with open(fpath, 'r') as data_file:
		data = json.load(data_file)

	return data


#*********************************Main*********************************
def main():
	args = read_arguments()
	input_fpath, input_folder = get_absfpath(args.input)
	output_fpath, output_folder = get_absfpath(args.output)

	print(input_fpath)

	# data_df = read_csv(input_fpath)

	
	# create_output(output_dict, output_fpath, output_folder)
	





if __name__ == '__main__':
	main()
