from pandas.core.frame import DataFrame
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer

from clean_data import create_output_CSV, read_tsv

# First time running: uncomment this and run it
# nltk.download('vader_lexicon')

def main():
  fpath: str = './../data/clean_data.tsv'
  tweets_df: DataFrame = read_tsv(fpath=fpath)
  tweets: list[str] = tweets_df['text'].to_list()

  positive: int = 0
  negative: int = 0
  neutral: int = 0
  polarity: int = 0
  tweet_list: list[str] = []
  neutral_list: list[str] = []
  negative_list: list[str] = []
  positive_list: list[str] = []

  for tweet in tweets:
    tweet_list.append(tweet)
    analysis: TextBlob = TextBlob(tweet)
    score: dict[str, float] = SentimentIntensityAnalyzer().polarity_scores(tweet)
    neg: float = score['neg']
    pos: float = score['pos']
    polarity += analysis.sentiment.polarity

    if neg > pos:
      negative_list.append(tweet)
      negative += 1
    elif pos > neg:
      positive_list.append(tweet)
      positive += 1

    elif pos == neg:
      neutral_list.append(tweet)
      neutral += 1

  print('Positive tweets: {}'.format(len(positive_list)))
  print('Negative tweets: {}'.format(len(negative_list)))
  print('Neutral tweets: {}'.format(len(neutral_list)))

  add_to_tsv(neutral_list, negative_list, positive_list)


def add_to_tsv(pos: list[str], neg: list[str], neu: list[str]):
  fpath: str = './../data/clean_data.tsv'
  file_output: str = '../data/clean_data_labeled.tsv'
  # fpath_output_dir: str = '../data/'
  df: DataFrame = read_tsv(fpath=fpath)
  df['status'] = None

  for tweet in pos:
    row_index = df[df['text'] == tweet].index
    df.loc[row_index, 'staus'] = 'pos'

  for tweet in neg:
    row_index = df[df['text'] == tweet].index
    df.loc[row_index, 'staus'] = 'neg'

  for tweet in neu:
    row_index = df[df['text'] == tweet].index
    df.loc[row_index, 'staus'] = 'neu'

  create_output_CSV(df=df, fpath=file_output, sep='\t')

if __name__ == '__main__':
  main()
