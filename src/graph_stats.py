from pandas.core.frame import DataFrame
import pandas as pd

from clean_data import get_topics, read_csv, read_tsv


def main():
  fpath: str = './../data/clean_data_annotated_1000.csv'
  df_tweets: DataFrame = read_csv(fpath=fpath)
  df_topics: DataFrame = get_topics(fpath=fpath)

  total_tweets: int = len(df_tweets)

  topics = pd.DataFrame(df_topics['topics'])['topics'].unique()
  for topic in topics:
    print(topic, " = ", calculate_status_ratio(df_tweets, topic=topic))


  print(calculate_number_topic(df_tweets))


def calculate_status_ratio(df: DataFrame, topic: str):
  pos, neg, neu = 0, 0, 0
  for index, row in df.iterrows():
    if row['topics'] == topic:
      if (row['status'] == 'pos'):
        pos += 1
      elif (row['status'] == 'neg'):
        neg += 1
      else:
        neu += 1
  return (pos, neg, neu)

def calculate_number_topic(df: DataFrame):
  print(df['topics'].value_counts())




if __name__ == '__main__':
  main()
