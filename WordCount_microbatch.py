import sys

import time

from flink.plan.Environment import get_environment
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.plan.Constants import FLOAT, WriteMode

import tweepy 
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from pymongo import MongoClient

consumer_key = '' 
consumer_secret = '' 
access_token = ''
access_secret = ''

tweets = list()

class StreamRead(StreamListener):
    def __init__(self):
        super().__init__()
        self.counter = 0
        self.limit = 10
        print("Collecting")

    def on_status(self, status):
        tweets.append(status.text)
        self.counter += 1
        if self.counter > self.limit:
            print("Stop")
            return False

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

class Adder(GroupReduceFunction):
    def reduce(self, iterator, collector):
        count, word = iterator.next()
        count += sum([x[0] for x in iterator])
        collector.collect((count, word))

def runner():
    env = get_environment()

    data = env.from_elements(tweets)
    # we first map each word into a (1, word) tuple, then flat map across that, and group by the key, and sum
    # aggregate on it to get (count, word) tuples
    data \
        .flat_map(lambda x, c: [(1, word) for word in x.lower().split()]) \
        .group_by(1) \
        .reduce_group(Adder(), combinable=True) \
        .output()

    # execute the plan locally.
    env.execute(local=True)


if __name__ == "__main__":
    #Connect to mongo
    #client = MongoClient('localhost', 27017)
    #Get database
    #db = client['DB_POD']
    #Get collection
    #cl = db['tweets']

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StreamRead()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    stream = Stream(auth, l)

    stream.filter(track=['donald', 'trump'])

    runner()