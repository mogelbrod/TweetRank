package ranker;

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
	private Hashtable<Long, Long> tweetVisited = new Hashtable<Long, Long>();

	/** Maps users to tweets */
	private Hashtable<Long, List<Long>> userTweets = new Hashtable<Long, List<Long>>();

	/** Contains all tweets */
	private HashSet<Long> tweet_set = new HashSet<Long>();
	private ArrayList<Long> tweets  = new ArrayList<Long>();

	/** Maps a tweet to a list of user mentions */
	private Hashtable<Long, List<Long>> mentioned = new Hashtable<Long, List<Long>>();

	/** Maps a user to a list of users he/she follows */
	private Hashtable<Long, List<Long>> follows = new Hashtable<Long, List<Long>>();

	/** Map a reply/retweet to the original tweet */
	private Hashtable<Long, Long> refTweets = new Hashtable<Long, Long>();

	/** Map a tweet to a list of hashtags */
	private Hashtable<Long, List<String>> hashtagsByTweet = new Hashtable<Long,List<String>>();
	private Hashtable<String, List<Long>> tweetsByHashtag = new Hashtable<String,List<Long>>();

	public void computePageRank() {
		int m = tweets.size()/10;
		MCCompletePath(m);
	}

	private synchronized void addTweet(Long tweetID) {
		if ( !tweet_set.contains(tweetID) ) {
			tweet_set.add(tweetID);
			tweets.add(tweetID);
		}
	}

	private synchronized void addAllTweets(List<Long> tweetIDs) {
		for (Long tid : tweetIDs)
			addTweet(tid);
	}

	public synchronized void addRefTweets(Long tweetID, Long refTweetID) {
		refTweets.put(tweetID, refTweetID);
		addTweet(tweetID);
		addTweet(refTweetID);
	}

	public synchronized void addUserTweets(Long userID, List<Long> tweetIDs) {
		List<Long> curr_list = userTweets.get(userID);
		if (curr_list == null) userTweets.put(userID, (curr_list = new ArrayList<Long>()));
		curr_list.addAll(tweetIDs);
		addAllTweets(tweetIDs);
	}

	public synchronized void addMentioned(Long tweetID, List<Long> userIDs) {
		List<Long> curr_list = mentioned.get(tweetID);
		if (curr_list == null) mentioned.put(tweetID, (curr_list = new ArrayList<Long>()));
		curr_list.addAll(userIDs);
		addTweet(tweetID);
	}

	public synchronized void addFollows(Long userID, List<Long> userIDs) {
		List<Long> curr_list = follows.get(userID);
		if (curr_list == null) follows.put(userID, (curr_list = new ArrayList<Long>()));
		curr_list.addAll(userIDs);
	}

	public synchronized void addHashtags(Long tweetID, List<String> hashtags) {
		List<String> curr_list = hashtagsByTweet.get(tweetID);
		if (curr_list == null) hashtagsByTweet.put(tweetID, (curr_list = new ArrayList<String>()));
		curr_list.addAll(hashtags);

		// Transpose the list
		for(String ht : hashtags) {
			List<Long> tweets = tweetsByHashtag.get(ht);
			if (tweets == null) tweetsByHashtag.put(ht, (tweets = new ArrayList<Long>()));
			tweets.add(tweetID);
		}

		addTweet(tweetID);
	}

	private void addVisit(Long tweetID) {
		Long c = tweetVisited.get(tweetID);
		if ( c == null ) c = 1L;
		else c = c + 1;
		tweetVisited.put(tweetID, c);
	}

	/** Jump to a random tweet. */
	private Long randomJump(Random r) {
		return tweets.get(r.nextInt(tweets.size()));
	}

	/** Jump to a random tweet from a random related user (mentioned/followed, just pass the appropiate list). */
	private Long jumpUserTweet(Long tweetID, Random r, List<Long> usersList) {
		// If there are no related users, then jump to a random tweet.
		if (usersList == null || usersList.size() == 0) return randomJump(r);

		Long randomUser = usersList.get(r.nextInt(usersList.size()));
		List<Long> tweetsOfUser = userTweets.get(randomUser);

		// If the related user does not have tweets, we jump to a random tweet.
		if (tweetsOfUser == null || tweetsOfUser.size() == 0) return randomJump(r);

		return tweetsOfUser.get(r.nextInt(tweetsOfUser.size()));
	}

	/** Jump to a random hashtag for the given tweet, and then to a random tweet for that hashtag. */
	private Long jumpHashtagTweet(Long tweetID, Random r) {
		List<String> tweet_hts = hashtagsByTweet.get(tweetID);
		if (tweet_hts == null || tweet_hts.size() == 0) return randomJump(r);

		String randomHashtag = tweet_hts.get(r.nextInt(tweet_hts.size()));
		List<Long> hashtag_tws = tweetsByHashtag.get(randomHashtag);
		return hashtag_tws.get(r.nextInt(hashtag_tws.size()));
	}

	/** Jump to the referenced tweet. */
	private Long jumpReferenceTweet(Long tweetID, Random r) {
		if ( !refTweets.containsKey(tweetID) ) return randomJump(r);
		else return refTweets.get(tweetID);
	}

	private void MCCompletePath(int m) {
		Random r = new Random();
		double random;

		for(Long currentID : tweets) {
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

		for(Entry<Long,Long> entry : tweetVisited.entrySet()) {
			Double tweetRank = entry.getValue()/(double)(tweets.size() * m);
			System.out.println(entry.getKey() + "\t" + tweetRank);
		}
	}
}
