package computer;

import graph.TemporaryGraph;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import java.util.concurrent.locks.ReentrantLock;

public class TweetRankComputer {
	private static final Logger logger = Logger.getLogger("ranker.logger");	
	/* Cummulative probabilities. TODO: adjust this parameters. */
	private static final double VISIT_REFERENCED_TWEET = 0.30; // 30%
	private static final double VISIT_MENTIONED_USER   = 0.55; // 25%
	private static final double VISIT_FOLLOWED_USER    = 0.65; // 10%
	private static final double VISIT_USED_HASHTAG     = 0.90; // 25%
	//private static final double RANDOM_JUMP            = 1.00; // 10%
	private Random r = new Random();
	private ReentrantLock cLock = new ReentrantLock();

	/** Mapping between a tweet and how many times it's been visited by the surfer */
	private HashMap<Long, Long> tweetVisited = new HashMap<Long, Long>();	

	private TemporaryGraph graph;
	
	public TweetRankComputer() {
		this.graph = null;
	}

	public void setTemporaryGraph(TemporaryGraph graph) {
		this.graph = graph;
	}
	
	public static class ConcurrentComputationException extends Exception {
		private static final long serialVersionUID = 6110756217026832483L;
	}
	
	public static class NullTemporaryGraphException extends Exception {
		private static final long serialVersionUID = -7656094713092868726L;
	}

	public HashMap<Long,Double> compute() throws ConcurrentComputationException, NullTemporaryGraphException {
		HashMap<Long,Double> tweetrank = null;
		if ( !cLock.tryLock() ) 
			throw new ConcurrentComputationException();
		if ( graph == null )
			throw new NullTemporaryGraphException();
		
		try {
			int m = graph.getNumberOfTweets()/5;
			tweetrank = MCCompletePath(m);
		} finally {
			cLock.unlock();
		}
		
		return tweetrank;
	}

	private void addVisit(Long tweetID) {
		Long c = tweetVisited.get(tweetID);
		if ( c == null ) c = 1L;
		else c = c + 1;
		tweetVisited.put(tweetID, c);
	}

	/** Jump to a random tweet from a random related user (mentioned/followed, just pass the appropiate list). */
	private Long jumpUserTweet(ArrayList<Long> usersList) {
		// If there are no related users, then jump to a random tweet.
		if (usersList == null || usersList.size() == 0) return graph.getRandomTweet(r);

		Long randomUser = usersList.get(r.nextInt(usersList.size()));
		ArrayList<Long> tweetsOfUser = graph.getUserTweets(randomUser);

		// If the related user does not have tweets, we jump to a random tweet.
		if (tweetsOfUser == null || tweetsOfUser.size() == 0) return graph.getRandomTweet(r);

		return tweetsOfUser.get(r.nextInt(tweetsOfUser.size()));
	}

	/** Jump to a random hashtag for the given tweet, and then to a random tweet for that hashtag. */
	private Long jumpHashtagTweet(Long tweetID) {
		ArrayList<String> tweet_hts = graph.getHashtagsByTweet(tweetID);
		if (tweet_hts == null || tweet_hts.size() == 0) return graph.getRandomTweet(r);

		String randomHashtag = tweet_hts.get(r.nextInt(tweet_hts.size()));
		ArrayList<Long> hashtag_tws = graph.getTweetsByHashtag(randomHashtag);
		return hashtag_tws.get(r.nextInt(hashtag_tws.size()));
	}

	/** Jump to the referenced tweet. */
	private Long jumpReferenceTweet(Long tweetID) {
		Long nid = graph.getRefTweet(tweetID);
		if (nid == null) return graph.getRandomTweet(r);
		else return nid;
	}
	
	private static String getCurrentDateTimeAsString() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
		Date date = new Date();
		return dateFormat.format(date);		
	}

	private HashMap<Long,Double> MCCompletePath(int m) {	
		logger.info("Ranking started at " + getCurrentDateTimeAsString());
		for(Long currentID : graph.getTweetSet()) {
			for(int i = 1; i <= m; ++i) {
				double random = r.nextDouble();
				addVisit(currentID);

				if ( random <= VISIT_REFERENCED_TWEET )
					currentID = jumpReferenceTweet(currentID);
				else if ( random <= VISIT_MENTIONED_USER )
					currentID = jumpUserTweet(graph.getMentionedUsers(currentID));
				else if ( random <= VISIT_FOLLOWED_USER )
					currentID = jumpUserTweet(graph.getFollowingUsers(graph.getTweetOwner(currentID)));
				else if ( random <= VISIT_USED_HASHTAG )
					currentID = jumpHashtagTweet(currentID);
				else
					currentID = graph.getRandomTweet(r);
			}
		}

		HashMap<Long,Double> pagerank = new HashMap<Long,Double>();
		for(Entry<Long,Long> entry : tweetVisited.entrySet()) {
			Double tweetRank = entry.getValue()/(double)(graph.getNumberOfTweets() * m);
			pagerank.put(entry.getKey(), tweetRank);
		}
		logger.info("Ranking finished at " + getCurrentDateTimeAsString());
		return pagerank;
	}
}
