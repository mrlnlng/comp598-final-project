'''
Script that parses the data collected and checks for:
	- Tweet uniqueness
	- Language: English
	- High likelihood of it being related to the movie

This script is specific about 'Shang-Chi and the Ten Rings'
movie.

Command to run: python clean_data.py -i ../data/collected_tweets_10000.tsv -o ../data/clean_data.tsv

Authors: Teresa Altamirano Mayoral, Parsa Yadollahi
'''
#*********************************Imports*********************************
import sys, os
import pandas as pd
import argparse
import json
import math
import networkx as nx
import numpy as np


#*********************************Functions*********************************
'''
Function that reads the arguments with flags -i and -o
'''
def read_arguments():
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--input', help='Absolute path of the input CSV file')
	parser.add_argument('-o', '--output', help='Absolut path of the output JSON file')

	return parser.parse_args()

'''
Function that return the absolute path of the file (fpath) and the absolute path of the
folder (folder).

This is in case the folder has to be created later on.
'''
def get_absfpath(input_fpath, store_location=''):
	curr_dir = os.path.dirname(os.path.abspath(__file__))
	folders = input_fpath.split('/')
	folder = os.path.join(curr_dir, store_location)
	for i in range(0, len(folders)-1):
		folder = os.path.join(folder, folders[i])

	fpath = os.path.join(folder, folders[-1])
	return fpath, folder

'''
Function creates the output folder (if necessary) and the JSON file
such that every line is a new JSON object.

Takes Dictionary, String, String, Boolean, Boolean
'''
def create_output_JSON(output_dict, fpath, folder='', overwrite=True, indent=True):
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

'''
Function creates the output folder (if necessary) and the CSV file.
If sep='\t' then it creates a TSV file.

Takes DataFrame, String, String, Boolean, String
'''
def create_output_CSV(df, fpath, folder='', overwrite=True, sep=','):
	if(overwrite):
		mode = 'w'
	else:
		mode = 'a'
	try:
		df.to_csv(fpath, sep=sep)
	except:
		# print(path)
		os.mkdir(folder)
		df.to_csv(fpath, sep=sep)

'''
Function reads a CSV given an absolute path and returns
a Data Frame

Takes String
Returns DataFrame
'''
def read_csv(fpath):
	df = pd.read_csv(fpath)

	return df

'''
Function reads a TSV given an absolute path and returns
a Data Frame

Takes String
Returns DataFrame
'''
def read_tsv(fpath):
	df = pd.read_csv(fpath, sep='\t')

	return df

'''
Function that goes from JSON file to dictionary.

Takes String (absolute path of JSON file)
Returns Dictionary
'''
def json_to_dict(fpath):
	with open(fpath, 'r') as data_file:
		data = json.load(data_file)

	return data

'''
Function that filters the DataFrame and
returns only the tweets in English

Takes DataFrame
Returns DataFrame
'''
def filter_language(df, language='en'):
	df = df.drop(df[df.lang != language].index)
	return df

'''
Function that adds a column 'isRelevant'.
A True value in 'isRelevant' is added
whenever a hashtag is in the target
hashtags.

Takes DataFrame
Returns DataFrame
'''
def isRelevant_hashtags(df, hashtags_list):
	num_rows = len(df.index)
	print(num_rows)
	isRelevant = False
	isRelevant_list = []

	for index, row in df.iterrows():
		isRelevant = False
		curr_hashtags = row['hashtags'].strip('[').strip(']').lower()

		try:
			curr_hashtags = curr_hashtags.split(',')
		except:
			print('No comma to remove')


		for c_h in curr_hashtags:
			c_h = c_h.strip("'").strip(' ').strip("'")
			if c_h in hashtags_list:
				isRelevant = True
				break

		isRelevant_list.append(isRelevant)

	df['isRelevant'] = isRelevant_list

	return df

def filter_hashtags(df):
	df = df[df.isRelevant]
	return df

'''
Function that filters the DataFrame and
returns only the tweets in that contain
tags @ in the given list.

Takes DataFrame
Returns DataFrame
'''
def filter_tags_tweet_text(df, at_list):
	pass

'''
Function that filters the DataFrame and
returns only the tweets in that contain
key words in the tweet text.

Takes DataFrame
Returns DataFrame
'''
def filter_tags_tweet_text(df, keywords_list):
	pass

#*********************************Main*********************************
def main():
	args = read_arguments()
	input_fpath, input_folder = get_absfpath(args.input)
	output_fpath, output_folder = get_absfpath(args.output)

	data_df = read_tsv(input_fpath)
	data_df = filter_language(data_df)

	hashtags_list = ['', 'shangchi', 'katychen', 'xialing', 'wenwu', 'mengerzhang', 'tenrings', 'thetenrings', 'simuliu',
						'awkwafina', 'meng', 'benkingsley', 'shangchiandthelegendofthetenrings', 'tonyleung', 'tonyleungchiuwai'
						'michelleyeoh', 'falachen', 'yingli', '']
	# potential_hashtags = ['henrylau', 'richbrian', 'alwaysrising', 'babasays']
	# tag_list = ['']
	# key_words = ['']

	data_df = isRelevant_hashtags(data_df, hashtags_list)
	data_df = filter_hashtags(data_df)

	print(data_df.head())

	create_output_CSV(data_df, output_fpath, sep='\t')


if __name__ == '__main__':
	main()
