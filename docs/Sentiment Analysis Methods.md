#Sentiment Analysis
Measure sentiment of given text in terms of a score where negative, neutral and positive sentiments are detected. Based on using lexicon libraries like AFINN to measure sentiment of individual words.

#Dependency Parsing
Build a list of word dependencies within a sentence along with relations using SpaCy or StanfordDependencyParser. 

#Algorithm

1. Pre-process hurricane sandy tweets using special character removal, stop-word removal, lemmatization and contraction expansion.
2. Run keyword clustering and searches to identify most important topics. (Topic modelling)
3. Use dependency parsing to identify dependencies of words with the keywords in the tweets.
4. Build a sentiment score on the basis of dependent phrases and relations to get specific emotion in the tweet.

#Example Code

- [Sentiment Analysis with Dependency Parsing](https://github.com/aryaman4/hyperpartisan-news/blob/sentiment/sentiment.py)