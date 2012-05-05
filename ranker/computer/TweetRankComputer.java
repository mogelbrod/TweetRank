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
	/* TODO: adjust this parameters. */
	private static final double VISIT_REFERENCED_TWEET_CUM = 0.30; // 30% 
	private static final double VISIT_MENTIONED_USER_CUM   = 0.60; // 30%
	private static final double VISIT_FOLLOWED_USER_CUM    = 0.70; // 10%
	private static final double VISIT_USED_HASHTAG_CUM     = 0.90; // 20%
	//private static final double RANDOM_JUMP_CUM            = 1.00; // 10%
	
	private static final int NUM_WORKERS               = 8;
	private int PATH_LENGTH;
	
	private Random rseed = new Random();
	private ReentrantLock cLock = new ReentrantLock(); // Avoids concurrent TweetRank computations
	private ReentrantLock vLock = new ReentrantLock(); // Avoids concurrent access to tweetVisited

	/** Mapping between a tweet and how many times it's been visited by the surfer */
	private HashMap<Long, Long> tweetVisited = new HashMap<Long, Long>();	
	
	/** Read-only graph used to compute the TweetRank. */
	private TemporaryGraph graph = null;

	public static class ConcurrentComputationException extends Exception {
		private static final long serialVersionUID = 6110756217026832483L;
	}

	public static class NullTemporaryGraphException extends Exception {
		private static final long serialVersionUID = -7656094713092868726L;
	}
	
	private static String getCurrentDateTimeAsString() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
		Date date = new Date();
		return dateFormat.format(date);		
	}

	private class WorkerThread implements Runnable {
		private int tidx;

		public WorkerThread(int tidx) {
			super();
			this.tidx = tidx;
		}

		@Override
		public void run() {
			List<Long> tweets = graph.getTweetList();
			for(int idx = tidx; idx < tweets.size(); idx += NUM_WORKERS) {
				Long currentID = tweets.get(idx);
				for(int i = 1; i <= PATH_LENGTH; ++i) {
					double random = rseed.nextDouble();
					addVisit(currentID);

					if ( random <= VISIT_REFERENCED_TWEET_CUM ) {
						currentID = jumpReferenceTweet(currentID);
						if (currentID != null) continue;
						else random = VISIT_REFERENCED_TWEET_CUM + rseed.nextDouble()*(1-VISIT_REFERENCED_TWEET_CUM);
					}

					if ( random <= VISIT_MENTIONED_USER_CUM ) {
						currentID = jumpUserTweet(graph.getMentionedUsers(currentID));
						if (currentID != null) continue;
						else random = VISIT_MENTIONED_USER_CUM + rseed.nextDouble()*(1-VISIT_MENTIONED_USER_CUM);
					}

					if ( random <= VISIT_FOLLOWED_USER_CUM ) {
						currentID = jumpUserTweet(graph.getFollowingUsers(graph.getTweetOwner(currentID)));
						if (currentID != null) continue;
						else random = VISIT_FOLLOWED_USER_CUM + rseed.nextDouble()*(1-VISIT_FOLLOWED_USER_CUM);
					}
					
					if ( random <= VISIT_USED_HASHTAG_CUM ) {
						currentID = jumpHashtagTweet(graph.getHashtagsByTweet(currentID));
						if (currentID != null) continue;
						else random = VISIT_USED_HASHTAG_CUM + rseed.nextDouble()*(1-VISIT_USED_HASHTAG_CUM);
					}
					
					currentID = graph.getRandomTweet(rseed);
				}
			}
		}
	}

	/**
	 * Adds a visit to the specified tweetID. 
	 * This method is thread-safe, multiple threads can add visits concurrently.
	 * @param tweetID Visited tweet id.
	 */
	private void addVisit(Long tweetID) {
		vLock.lock();
		try {
			Long c = tweetVisited.get(tweetID);
			if ( c == null ) c = 1L;
			else c = c + 1;
			tweetVisited.put(tweetID, c);
		} finally {
			vLock.unlock();
		}
	}

	/** 
	 * Jumps to a random tweet from a random related user (mentioned/followed, just pass the appropiate list).
	 * @param userList List of users to select a random user.
	 * @return New tweet id. Returns null if the passed list is empty or the selected random user has no tweets.
	 */
	private Long jumpUserTweet(ArrayList<Long> usersList) {
		// If there are no related users, then jump to a random tweet.
		if (usersList == null || usersList.size() == 0) return null;

		Long randomUser = usersList.get(rseed.nextInt(usersList.size()));
		ArrayList<Long> tweetsOfUser = graph.getUserTweets(randomUser);

		// If the related user does not have tweets, we jump to a random tweet.
		if (tweetsOfUser == null || tweetsOfUser.size() == 0) return null;

		return tweetsOfUser.get(rseed.nextInt(tweetsOfUser.size()));
	}

	/** 
	 * Jumps to a random hashtag for the given tweet, and then to a random tweet for that hashtag. 
	 * @param tweetHashtags List of hashtags included in a tweet.
	 * @return New tweet id. Returns null if the tweet has no hashtags associated.
	 */
	private Long jumpHashtagTweet(ArrayList<String> tweetHashtags) {
		if (tweetHashtags == null || tweetHashtags.size() == 0) return graph.getRandomTweet(rseed);

		String randomHashtag = tweetHashtags.get(rseed.nextInt(tweetHashtags.size()));
		ArrayList<Long> hashtag_tws = graph.getTweetsByHashtag(randomHashtag);
		return hashtag_tws.get(rseed.nextInt(hashtag_tws.size()));
	}

	/** 
	 * Jumps to a referenced (replied or retweeted) tweet.
	 * @param tweetID Current tweetID.
	 * @return New tweet id referenced by tweetID. Returns null if the tweet has no references.
	 */
	private Long jumpReferenceTweet(Long tweetID) {
		return graph.getRefTweet(tweetID);
	}
	
	/**
	 * Starts the computation of the TweetRank.
	 * @return A HashMap where each entry is a pair (TweetID, TweetRank). If any problem
	 * ocurred, the result is null.
	 * @throws ConcurrentComputationException is thrown if there is a concurrent computation.
	 * @throws NullTemporaryGraphException if the graph was not initialized.
	 */
	public HashMap<Long,Double> compute() throws ConcurrentComputationException, NullTemporaryGraphException {
		// Check if there is another thread already computing the tweetrank
		if ( !cLock.tryLock() ) 
			throw new ConcurrentComputationException();
		
		// Check if the graph is null
		if ( graph == null )
			throw new NullTemporaryGraphException();
		
		HashMap<Long,Double> tweetrank = null;

		try {
			PATH_LENGTH = graph.getNumberOfTweets()/100;
			if (PATH_LENGTH < 100) PATH_LENGTH = 100;
			tweetrank = MCCompletePath();
		} finally {
			cLock.unlock();
		}

		return tweetrank;
	}	

	/** 
	 * Monte Carlo method that computes an approximation to the TweetRank.
	 * Multiple threads are created to perform the computation and improve its execution time.
	 * @return A HashMap where each entry is a pair (TweetID, TweetRank). If any problem
	 * ocurred, the result is null.
	 */
	private HashMap<Long,Double> MCCompletePath() {	
		logger.info("Ranking started at " + getCurrentDateTimeAsString());

		// Create workers
		Thread[] workers = new Thread[NUM_WORKERS];     

		// Start workers
		for(int widx = 0; widx < NUM_WORKERS; ++widx) { 
			workers[widx] = new Thread(new WorkerThread(widx));
			workers[widx].start();
		}

		// Wait until all the workers have finished
		boolean interrupted = false;
		for(int widx = 0; widx < NUM_WORKERS; ++widx) {
			try 
			{ workers[widx].join();	} 
			catch (InterruptedException e)
			{ logger.info("Interrupted thread", e); interrupted = true; }
		}
		
		// If any worker was interrupted, discard the computation.
		if (interrupted) return null;

		// Normalization
		HashMap<Long,Double> pagerank = new HashMap<Long,Double>();
		for(Entry<Long,Long> entry : tweetVisited.entrySet()) {
			Double tweetRank = entry.getValue()/(double)(graph.getNumberOfTweets() * PATH_LENGTH);
			pagerank.put(entry.getKey(), tweetRank);
		}
		
		logger.info("Ranking finished at " + getCurrentDateTimeAsString());
		return pagerank;
	}
	
	/** Set the temporary graph. */
	public void setTemporaryGraph(TemporaryGraph graph) {
		cLock.lock();
		this.graph = graph;
		cLock.unlock();
	}
}
