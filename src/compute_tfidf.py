
import json
import emoji
import nltk
from nltk.corpus import stopwords
from pandas.core.frame import DataFrame
import re
from clean_data import get_topics, json_pretty_print
import math

'''
  Using the same TFIDF as in assignment 8
  i.e.
    tf-idf(word, category) = tf(w, category) x idf(word)

    tf(word, category) = the number of times that word appears in that category

    idf(word) = log [
      (total number categories) /
      (number of categories using the word)
    ]
'''

def main() -> None:
  dialogs: dict = compute_dialogs()
  tfidf_dict: dict = compute_tfidf(dialogs)
  # json_pretty_print(tfidf_dict)


'''
  computes the tfidf
  category1: {
    word1: <tfidf score>
    word2: <tfidf score>
    ...
    word10: <tfidf score>
  },
  ...
  category8: {
    word1: <tfidf score>
    word2: <tfidf score>
    ...
    word10: <tfidf score>
  },
'''
def compute_tfidf(dialogs: dict) -> dict:
  num_categories = len(dialogs.keys())
  tfidf_dict = {}

  for cat in dialogs.keys():
    tfidf_dict[cat] = {}
    words = dialogs[cat]

    for word in words:

      num_cat_using_word_w = 0
      for c in dialogs.keys():
        if word in dialogs[c]:
          num_cat_using_word_w += 1

      # Number occurance of word in category
      tf = dialogs[cat][word]
      # Number of categories the word apprears in
      idf = math.log(num_categories / num_cat_using_word_w)
      tfidf = tf * idf

      tfidf_dict[cat][word] = tfidf

  tfidf_dict_top = {}
  for category in dialogs.keys():
    tfidf_dict_top[category] = {}
    sorted_tf_idf_scores = sorted((tfidf_dict[category]).items(), key=lambda x: x[1], reverse=True)[:10]
    for (word, score) in sorted_tf_idf_scores:
      tfidf_dict_top[category][word] = round(score, 3)

  json_pretty_print(tfidf_dict_top)
  return tfidf_dict_top


'''
  computes the word count for each category
  category1: {
    word1: <# times appears>
    word2: <# times appears>
    ...
    wordn: <# times appears>
  },
  ...
  category8: {
    word1: <# times appears>
    word2: <# times appears>
    ...
    wordn: <# times appears>
  },
'''
def compute_dialogs() -> dict:
  fpath: str = './../data/clean_data_annotated_1000.csv'
  df: DataFrame = get_topics(fpath=fpath)

  dialogs = {}

  for index, row in df.iterrows():
    # topic = row['topics']
    tweet = clean_tweet(row['text'])
    words = tweet.split()
    topic = row['topics']
    # Initialize key topic: {}
    if topic not in dialogs:
      dialogs[topic] = {}

    # Add word to topic: {word1: #, word2: #,...}
    for word in words:
      if word not in dialogs[topic]:
        dialogs[topic][word] = 1
      else:
        dialogs[topic][word] += 1

  return dialogs




def clean_tweet(tweet: str) -> str:
  stop_set = set(stopwords.words('english'))

  tweet: str = tweet[2:] # [:2] to remove b'
  # import pdb; pdb.set_trace()
  tweet: str = re.sub("@[A-Za-z0-9]+","", tweet) # Remove @ sign
  tweet: str = re.sub(r"(?:\@|http?\://|https?\://|www)\S+", "", tweet) # Remove http links
  tweet: str = " ".join(tweet.split())
  tweet: str = ''.join(c for c in tweet if c not in emoji.UNICODE_EMOJI) # Remove Emojis
  tweet: str = tweet.replace("#", "").replace("_", " ").replace("-", "") # Remove hashtag sign but keep the text
  tweet: str = " ".join(w.lower() for w in nltk.wordpunct_tokenize(tweet) if w.isalpha())
  tweet: str = ' '.join([word for word in tweet.split() if word not in stop_set])
  tweet: str = ' '.join([word for word in tweet.split() if len(word) > 2])
  return tweet

if __name__ == '__main__':
  main()
