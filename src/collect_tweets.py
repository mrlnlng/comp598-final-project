import requests
import datetime
from dateutil import parser
import argparse
import json
import os
from os import path
import csv

def load_args():
	parser = argparse.ArgumentParser()
	parser.add_argument('-o', "--output", required = True, dest = "output")
	parser.add_argument('-q', "--query", required = True, dest = "query")
	parser.add_argument('-n', "--num", required = True, dest = "num")
	args = parser.parse_args()
	return (args.output, int(args.num), args.query)

def generate_url(query_dict,token=None):
	generated_string = '&'.join([f'{key}={value}' for key, value in query_dict.items()])
	if token is None:
		return f"https://api.twitter.com/2/tweets/search/recent?{generated_string}"
	else:
		return f"https://api.twitter.com/2/tweets/search/recent?next_token={token}&{generated_string}"


def get_token(token_path="../token.dev"):
	with open(token_path,"r") as f:
		return f.read()
	
def make_request(url,request_token=get_token()):
	payload={}
	all_data = []
	headers = {
	'Authorization': request_token,
	'Cookie': 'guest_id=v1%3A163737786669411976; guest_id_ads=v1%3A163737786669411976; guest_id_marketing=v1%3A163737786669411976; personalization_id="v1_aWpM9WOyDOP2MOKXoniauQ=="'
	}
	response = requests.request("GET", url, headers=headers, data=payload)
	json_data =  response.json()
	return json_data

def add_day(date):
	parsed_date = parser.parse(date)
	added_date = parsed_date + datetime.timedelta(days=1)
	utc_string = added_date.strftime('%Y-%m-%dT%H:%M:%SZ')
	return utc_string
	

def collect_day_tweets(query_dict, token, count):
	all_data = []
	url = generate_url(query_dict, token)
	json_data = make_request(url)
	all_data.extend(json_data['data'])
	while len(all_data) < count and 'next_token' in json_data['meta']:
		token = json_data['meta']['next_token']
		url = generate_url(query_dict,token)
		json_data = make_request(url)
		all_data.extend(json_data['data'])

	return all_data[0:count]
# collects tweets
def collect_tweets(query_dict, count, days):
	initial_token = None
	all_data = []
	for i in range(days):
		json_data = collect_day_tweets(query_dict, initial_token, count // days + 1)
		query_dict["start_time"] = query_dict["end_time"]
		query_dict["end_time"] = add_day(query_dict["end_time"])
		all_data.extend(json_data)
	
	return all_data[0:count]

def check_path (output):
	real_path = os.path.realpath(output)
	directory = os.path.dirname(real_path)
	if not os.path.exists(directory):
		os.makedirs(directory)

def get_field_values(line):
	tweet_id = line.get('id')
	created_at = line['created_at']
	text = line.get('text').encode('unicode_escape')
	lang = line['lang']
	hashtag_clean = []
	if "entities" in line:
		entities = line['entities']
		if "hashtags" in entities:
			hashtags = entities["hashtags"]
			for hashtag in hashtags:
				hashtag_clean.append(hashtag["tag"])

	return [tweet_id, created_at, text, lang, hashtag_clean]

def generate_output(output, data):
	check_path(output)
	fields = ['id', 'created_at', 'text', 'lang', 'hashtags']
	with open(output, "w") as f:
		writer = csv.writer(f, delimiter='\t')
		writer.writerow(fields)
		for line in data:
			values = get_field_values(line)
			writer.writerow(values)	

def main():
	start_time = "2021-11-16T00:00:00Z"
	end_time = add_day(start_time)
	days = 3
	output, count, query = load_args()
	query_dict = {"start_time": start_time,
	"end_time": end_time, "max_results": 100, "query": query, "tweet.fields":"created_at,entities,lang"
	}
	data = collect_tweets(query_dict, count, days)
	generate_output(output, data)


if __name__ == "__main__":
	main()