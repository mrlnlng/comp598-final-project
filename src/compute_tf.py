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


def main():
  ...


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
def compute_dialogs():
  ...


if __name__ == '__main__':
  main()
