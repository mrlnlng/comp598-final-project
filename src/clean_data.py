'''
Script that parses the data collected and checks for:
	- Tweet uniqueness
	- Language: English
	- High likelihood of it being related to the movie

This script is specific about 'Shang-Chi and the Ten Rings'
movie.

Command to run: python clean_data.py -i ../data/collected_tweets_10000.tsv -o ../data/clean_data.tsv

Author: Teresa Altamirano Mayoral
'''
#*********************************Imports*********************************
import sys, os
import pandas as pd
import argparse
import json
import math
import networkx as nx
import numpy as np
# from datetime import datetime
# from datetime import date
import datetime
from pandas.core.frame import DataFrame
import pytz
from collections import Counter



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
		df.to_csv(fpath, sep=sep, index=False)
	except:
		# print(path)
		os.mkdir(folder)
		df.to_csv(fpath, sep=sep, index=False)

'''
Function reads a CSV given an absolute path and returns
a Data Frame

Takes String
Returns DataFrame
'''
def read_csv(fpath) -> DataFrame:
	df: DataFrame = pd.read_csv(fpath)

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
	# print(num_rows)
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

'''
Function returns a DataFrame containing
only the rows where 'isRelevant' is
True.

Takes DataFrame
Returns DataFrame
'''
def filter_hashtags(df):
	df = df[df.isRelevant]
	return df

'''
Function that filters the DataFrame and
removes the repeated tweets.
The tweets are filtered depending on
the id.

Takes DataFrame
Returns DataFrame
'''
def remove_duplicate_tweets(df):
	df = df.drop_duplicates(subset=['text'])
	# print(df.head())
	return df

'''
Function that filters the DataFrame and
returns only the tweets in that contain
key words in the tweet text.

Takes DataFrame
Returns DataFrame
'''
def filter_tags_tweet_text(df_nohashtag, keywords_list):
	isRelevant = False
	isRelevant_list = []

	for i,row in df_nohashtag.iterrows():
		#Check if this row has hashtags
		curr_hashtags = row['hashtags'].strip('[').strip(']').split(',')
		new_hash = []
		for h in curr_hashtags:
			new_hash.append(h.strip("'").strip(' ').strip("'").lower())
		# print(new_hash[0])
		if(new_hash[0] == ""):
			# print(curr_hashtags)
			# print("No hashtags")
			curr_text = row['text'].strip('b').strip("'").split()
			for w in curr_text:
				if w in keywords_list:
					isRelevant = True
					break
		else:
			isRelevant = True

		isRelevant_list.append(isRelevant)
		isRelevant = False

	df_nohashtag['isRelevant_text'] = isRelevant_list
	return df_nohashtag

'''
Function returns a DataFrame containing
only the rows where 'isRelevant_text' is
True.

Takes DataFrame
Returns DataFrame
'''
def filter_text(df):
	df = df[df.isRelevant_text]
	return df

'''
Function that creates a file of
all the tweets without hashtags.

Takes DataFrame
Returns -
'''
def get_no_hashtag_sample(df):
	texts = []
	for i,row in df.iterrows():
		#Check if this row has hashtags
		curr_hashtags = row['hashtags'].strip('[').strip(']').split(',')
		new_hash = []
		for h in curr_hashtags:
			new_hash.append(h.strip("'").strip(' ').strip("'").lower())
		if(new_hash[0] == ""):
			# curr_text = row['text']
			texts.append([row['id'], row['text'],row['hashtags']])

	df = pd.DataFrame(texts, columns=['id','text', 'hashtags'])
	create_output_CSV(df, "noHashtag.tsv", folder='../data/', sep='\t')


'''
Function that transforms the dates
in TSV into datetime objects. Returns
a list of dates of the form:
YEAR-MONTH-DAY

Takes DataFrame
Returns List
'''
def to_datetime(df):
	dates = []
	for i, row in df.iterrows():
		# print(datetime.strptime(row['created_at'][:-1], '%Y-%m-%dT%H:%M:%S.%f'))
		try:
			dt = datetime.datetime.strptime(row['created_at'][:-1], '%Y-%m-%dT%H:%M:%S.%f')
			d_truncated = datetime.date(dt.year, dt.month, dt.day)
			# print(d_truncated)
			dates.append(d_truncated)
		except ValueError as e:
			print('invalid date')
	return dates

'''
Function that prints and outputs a dictionary
of the days that are in the df with the number
of tweets per day.

Takes DataFrame
Returns Dictionary
'''
def get_number_of_rows_per_day(df):
	dates = to_datetime(df)
	count = dict(Counter(dates))
	print(count)

'''
Function that prints and outputs the top 8
topics found in the dataframe in a span of 200 tweets

Takes String (absolute path of Dataframe file)
Returns None
'''
def print_topics(fpath: str) -> None:
  df = read_tsv(fpath=fpath)
  labels = pd.DataFrame(df['topics'])

  json_pretty_print(Counter(labels.dropna(axis=0)['topics'].to_list()))

'''
Function that returns the top 8 topics
found in the dataframe in a span of 200 tweets

Takes String (absolute path of Dataframe file)
Returns Dataframe
'''
def get_topics(fpath: str) -> DataFrame:
  df = read_csv(fpath=fpath)
  return df[df['topics'].notna()]

'''
  Function that pretty prints a dictionary
  into proper JSON format
'''
def json_pretty_print(d, sort_keys=False) -> None:
  print(json.dumps(d ,indent=4, sort_keys=sort_keys))



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
	key_words = ['', 'shangchi', 'katychen', 'xialing', 'wenwu', 'mengerzhang', 'tenrings', 'thetenrings', 'simuliu',
						'awkwafina', 'meng', 'benkingsley', 'shangchiandthelegendofthetenrings', 'tonyleung', 'tonyleungchiuwai'
						'michelleyeoh', 'falachen', 'yingli', '@shangchi', '@simuliu', 'shang chi', 'shang-chi']

	data_df = isRelevant_hashtags(data_df, hashtags_list)
	data_df = filter_hashtags(data_df)
	data_df = remove_duplicate_tweets(data_df)

	# get_no_hashtag_sample(data_df)

	data_df = filter_tags_tweet_text(data_df, key_words)
	data_df = filter_text(data_df)
	data_df.reset_index(drop=True, inplace=True)

	get_number_of_rows_per_day(data_df)


	create_output_CSV(data_df, output_fpath, sep='\t')


if __name__ == '__main__':
	main()
