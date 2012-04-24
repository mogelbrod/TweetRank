
from RankerNotifier import RankerNotifier
from SolrNotifier import SolrNotifier
from Tweet import Tweet
from os import listdir
from xml.dom.minidom import parse
import sys



def usage():
    print 'python DirectoryNotifier.py data_dir'

def main(argv):
    if len(argv) != 2:
        usage()
        return -1

    rnotif = RankerNotifier()
    snotif = SolrNotifier()
    
    data_dir = argv[1]
    tweets_dir = data_dir + '/tweets/'
    users_dir  = data_dir + '/users/'
    """
    for fname in listdir(users_dir):
        user_id = int(fname.split('.')[0])
        ftype   = fname.split('.')[1]
        fname = users_dir + fname
        
        if ftype == 'friends':
            f = open(fname, 'r')
            friends = []
            for l in f:
                friends.append(int(l))
            rnotif.add_following(user_id, friends)
            f.close()
    """
    ntweets = 0
    for fname in listdir(tweets_dir):
        fname = tweets_dir + fname
        dom = parse(fname)
        tweets = []
        tweets_by_uid={}
        for tweet in dom.getElementsByTagName('status'):
            tweet = Tweet(tweet)
            tweets.append(tweet)
            """
            # Ranker notification
            if tweet.get_retweeted_status() is not None:
                rnotif.add_retweet(tweet.get_tweet_id(), tweet.get_retweeted_status().get_tweet_id())

            if tweet.get_replied_id() is not None:
                rnotif.add_reply(tweet.get_tweet_id(), tweet.get_replied_id())

            if len(tweet.get_mentioned_ids()) > 0:
                rnotif.add_mentions(tweet.get_tweet_id(), tweet.get_mentioned_ids())

            if len(tweet.get_hashtags()) > 0:
                rnotif.add_tweet_hashtags(tweet.get_tweet_id(), tweet.get_hashtags())
            """
            if not tweet.get_user_id() in tweets_by_uid:
                tweets_by_uid[tweet.get_user_id()] = set([tweet.get_tweet_id()])
            else:
                tweets_by_uid[tweet.get_user_id()].add(tweet.get_tweet_id())
        """
        for item in tweets_by_uid.items():
            rnotif.add_user_tweets(item[0], item[1])
        """
        # SolrNotifier
        snotif.add_tweets(tweets)
        ntweets = ntweets + len(tweets)
        if ntweets >= 10000:
            break

    snotif.flush()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
