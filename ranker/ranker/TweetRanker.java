
import java.util.*;
import java.util.Map.Entry;

public class TweetRanker {
    /* Cummulative probabilities. TODO: adjust this parameters. */
    private final double VISIT_REFERENCED_TWEET = 0.30; // 30%
    private final double VISIT_MENTIONED_USER   = 0.55; // 25%
    private final double VISIT_FOLLOWED_USER    = 0.65; // 10%
    private final double VISIT_USED_HASHTAG     = 0.90; // 25%
    private final double RANDOM_JUMP            = 1.00; // 10%

    /** Mapping between a tweet and how many times it's been visited by the surfer */
    private Hashtable<Integer, Integer> tweetVisited = new Hashtable<Integer, Integer>();

    /** Maps users to tweets */
    private Hashtable<Integer, List<Integer>> userTweets = new Hashtable<Integer, List<Integer>>();

    /** Contains all tweets */
    private HashSet<Integer> tweet_set = new HashSet<Integer>();
    private ArrayList<Integer> tweets  = new ArrayList<Integer>();

    /** Maps a tweet to a list of user mentions */
    private Hashtable<Integer, List<Integer>> mentioned = new Hashtable<Integer, List<Integer>>();

    /** Maps a user to a list of users he/she follows */
    private Hashtable<Integer, List<Integer>> follows = new Hashtable<Integer, List<Integer>>();

    /** Map a reply/retweet to the original tweet */
    private Hashtable<Integer, Integer> refTweets = new Hashtable<Integer, Integer>();

    /** Map a tweet to a list of hashtags */
    private Hashtable<Integer, List<String>> hashtagsByTweet = new Hashtable<Integer,List<String>>();
    private Hashtable<String, List<Integer>> tweetsByHashtag = new Hashtable<String,List<Integer>>();

    public void computePageRank() {
	int m = tweets.size()/10;
	MCCompletePath(m);
    }

    private synchronized void addTweet(Integer tweetID) {
	if ( !tweet_set.contains(tweetID) ) {
	    tweet_set.add(tweetID);
	    tweets.add(tweetID);
	}
    }

    private synchronized void addAllTweets(List<Integer> tweetIDs) {
	for (Integer tid : tweetIDs)
	    addTweet(tid);
    }

    public synchronized void addRefTweets(Integer tweetID, Integer refTweetID) {
	refTweets.put(tweetID, refTweetID);
	addTweet(tweetID);
	addTweet(refTweetID);
    }

    public synchronized void addUserTweets(Integer userID, List<Integer> tweetIDs) {
	List<Integer> curr_list = userTweets.get(userID);
	if (curr_list == null) userTweets.put(userID, (curr_list = new ArrayList<Integer>()));
	curr_list.addAll(tweetIDs);
	addAllTweets(tweetIDs);
    }

    public synchronized void addMentioned(Integer tweetID, List<Integer> userIDs) {
	List<Integer> curr_list = mentioned.get(tweetID);
	if (curr_list == null) mentioned.put(tweetID, (curr_list = new ArrayList<Integer>()));
	curr_list.addAll(userIDs);
	addTweet(tweetID);
    }

    public synchronized void addFollows(Integer userID, List<Integer> userIDs) {
	List<Integer> curr_list = follows.get(userID);
	if (curr_list == null) follows.put(userID, (curr_list = new ArrayList<Integer>()));
	curr_list.addAll(userIDs);
    }

    public synchronized void addHashtags(Integer tweetID, List<String> hashtags) {
	List<String> curr_list = hashtagsByTweet.get(tweetID);
	if (curr_list == null) hashtagsByTweet.put(tweetID, (curr_list = new ArrayList<String>()));
	curr_list.addAll(hashtags);

	// Transpose the list
	for(String ht : hashtags) {
	    List<Integer> tweets = tweetsByHashtag.get(ht);
	    if (tweets == null) tweetsByHashtag.put(ht, (tweets = new ArrayList<Integer>()));
	    tweets.add(tweetID);
	}

	addTweet(tweetID);
    }

    private void addVisit(Integer tweetID) {
	Integer c = tweetVisited.get(tweetID);
	if ( c == null ) c = 1;
	else c = c + 1;
	tweetVisited.put(tweetID, c);
    }

    /** Jump to a random tweet. */
    private Integer randomJump(Random r) {
	return tweets.get(r.nextInt(tweets.size()));
    }

    /** Jump to a random tweet from a random related user (mentioned/followed, just pass the appropiate list). */
    private Integer jumpUserTweet(Integer tweetID, Random r, List<Integer> usersList) {
	// If there are no related users, then jump to a random tweet.
	if (usersList == null || usersList.size() == 0) return randomJump(r);

	Integer randomUser = usersList.get(r.nextInt(usersList.size()));
	List<Integer> tweetsOfUser = userTweets.get(randomUser);

	// If the related user does not have tweets, we jump to a random tweet.
	if (tweetsOfUser == null || tweetsOfUser.size() == 0) return randomJump(r);

	return tweetsOfUser.get(r.nextInt(tweetsOfUser.size()));
    }

    /** Jump to a random hashtag for the given tweet, and then to a random tweet for that hashtag. */
    private Integer jumpHashtagTweet(Integer tweetID, Random r) {
	List<String> tweet_hts = hashtagsByTweet.get(tweetID);
	if (tweet_hts == null || tweet_hts.size() == 0) return randomJump(r);

	String randomHashtag = tweet_hts.get(r.nextInt(tweet_hts.size()));
	List<Integer> hashtag_tws = tweetsByHashtag.get(randomHashtag);
	return hashtag_tws.get(r.nextInt(hashtag_tws.size()));
    }

    /** Jump to the referenced tweet. */
    private Integer jumpReferenceTweet(Integer tweetID, Random r) {
	if ( !refTweets.containsKey(tweetID) ) return randomJump(r);
	else return refTweets.get(tweetID);
    }

    private void MCCompletePath(int m) {
	Random r = new Random();
	double random;

	for(Integer currentID : tweets) {
	    for(int i = 1; i <= m; ++i) {
		random = r.nextDouble();

		if ( random <= VISIT_REFERENCED_TWEET )
		    currentID = jumpReferenceTweet(currentID, r);
		else if ( random <= VISIT_MENTIONED_USER )
		    currentID = jumpUserTweet(currentID, r, mentioned.get(currentID));
		else if ( random <= VISIT_FOLLOWED_USER )
		    currentID = jumpUserTweet(currentID, r, follows.get(currentID));
		else if ( random <= VISIT_USED_HASHTAG )
		    currentID = jumpHashtagTweet(currentID, r);
		else
		    currentID = randomJump(r);

		addVisit(currentID);
	    }
	}

	for(Entry<Integer,Integer> entry : tweetVisited.entrySet()) {
	    Double tweetRank = entry.getValue()/(double)(tweets.size() * m);
	    System.out.println(entry.getKey() + "\t" + tweetRank);
	}
    }
}
