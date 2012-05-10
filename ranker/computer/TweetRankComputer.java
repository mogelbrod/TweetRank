package computer;

import graph.TemporaryGraph;
import utils.Time;

import java.util.*;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import java.util.concurrent.locks.ReentrantLock;

public class TweetRankComputer {
	private static final Logger logger = Logger.getLogger("ranker.logger");	
	/* TODO: adjust this parameters. */
	private static final double VISIT_REFERENCED_TWEET_CUM = 0.50; // 50% 
	private static final double VISIT_MENTIONED_USER_CUM   = 0.75; // 25%
	private static final double VISIT_FOLLOWED_USER_CUM    = 0.80; //  5%
	private static final double VISIT_USED_HASHTAG_CUM     = 0.95; // 15%
	private static final double VISIT_RANDOM_TWEET_CUM     = 1.00; //  5%
	private static final double BORED_PROBABILITY          = 0.20;

	private int M = 100;

	private ReentrantLock cLock = new ReentrantLock(); // Avoids concurrent TweetRank computations

	/** Read-only graph used to compute the TweetRank. */
	private TemporaryGraph graph = null;

	public static enum State {
		WORKING, IDLE
	}

	private State state = State.IDLE;
	private Date StartEndDate[] = new Date[2];

	/**
	 * This exception is thrown when a TweetRank computation attemps to start when
	 * an other has not finished yet.
	 */
	public static class ConcurrentComputationException extends Exception {
		private static final long serialVersionUID = 6110756217026832483L;
	}

	/**
	 * This exception is thrown when a TweetRank computation attemps to start
	 * but the Temporary Graph has not been initialized.
	 */
	public static class NullTemporaryGraphException extends Exception {
		private static final long serialVersionUID = -7656094713092868726L;
	}

	//private int tidx;
	private HashMap<Long,Long> counter = new HashMap<Long, Long> ();
	private Random rseed = new Random();

	/**
	 * Adds a visit to the specified tweetID. 
	 * This method is thread-safe, multiple threads can add visits concurrently.
	 * @param tweetID Visited tweet id.
	 */
	private void addVisit(Long tweetID) {
		Long c = counter.get(tweetID);
		if ( c == null ) counter.put(tweetID, 1L);
		else counter.put(tweetID, c+1);
	}

	/** 
	 * Jumps to a random tweet from a random related user (mentioned/followed, just pass the appropiate list).
	 * @param userList List of users to select a random user.
	 * @return New tweet id. Returns null if the passed list is empty or the selected random user has no tweets.
	 */
	private Long jumpUserTweet(Long TweetID, ArrayList<Long> usersList, long type) {
		// If there are no related users, then jump to a random tweet.
		if (usersList == null || usersList.size() == 0) {
			if (type == 0) logger.debug("EMPTY_UL (FW): Owner of tweet " + TweetID + " has no friends.");
			else logger.debug("EMPTY_UL (MN): Tweet " + TweetID + "has no mentions.");
			return null;
		}

		Long randomUser = usersList.get(rseed.nextInt(usersList.size()));
		ArrayList<Long> tweetsOfUser = graph.getUserTweets(randomUser);

		// If the related user does not have tweets, we jump to a random tweet.
		if (tweetsOfUser == null || tweetsOfUser.size() == 0) {
			logger.debug("SILENT_USER: User " + randomUser + " has no tweets.");
			return null;
		}

		return tweetsOfUser.get(rseed.nextInt(tweetsOfUser.size()));
	}

	/** 
	 * Jumps to a random hashtag for the given tweet, and then to a random tweet for that hashtag. 
	 * @param tweetHashtags List of hashtags included in a tweet.
	 * @return New tweet id. Returns null if the tweet has no hashtags associated.
	 */
	private Long jumpHashtagTweet(Long TweetID, ArrayList<String> tweetHashtags) {
		if (tweetHashtags == null || tweetHashtags.size() == 0) {
			logger.debug("EMPTY_HT: Tweet " + TweetID + " has no hashtags.");
			return null;
		}

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
	public TreeMap<Long,Double> compute() throws ConcurrentComputationException, NullTemporaryGraphException {
		cLock.lock();
		TreeMap<Long,Double> tweetrank = null;
		try {
			// Check if the graph is null
			if ( graph == null )
				throw new NullTemporaryGraphException();

			// Determine the path length to be used
			M = graph.getNumberOfTweets()/100;
			if (M < 100) M = 100;

			// Start computation!
			tweetrank = MCCompletePath();
		} finally {
			cLock.unlock();
		}
		return tweetrank;
	}	

	/** 
	 * Monte Carlo method that computes an approximation to TweetRank.
	 * Multiple threads are created to perform the computation and improve its execution time.
	 * @return A HashMap where each entry is a pair (TweetID, TweetRank). If any problem
	 * ocurred, the result is null.
	 */
	private TreeMap<Long,Double> MCCompletePath() {	
		logger.info("Ranking started at " + Time.formatDate("yyyy/MM/dd HH:mm:ss", new Date()));

		// Work started...
		state = State.WORKING;
		StartEndDate[0] = new Date();

		for(Long tw : graph.getTweetList()) {
			for(int i = 1; i <= M; ++i) {
				Long currentID = tw;

				do {
					double random = rseed.nextDouble();
					addVisit(currentID);
					Long nextID = null;

					if ( nextID == null && random <= VISIT_REFERENCED_TWEET_CUM ) {
						nextID = jumpReferenceTweet(currentID);
						if (nextID != null) logger.debug("JUMP: REF " + currentID + "->" + nextID);
						else random = VISIT_REFERENCED_TWEET_CUM + rseed.nextDouble()*(1-VISIT_REFERENCED_TWEET_CUM);
					}

					if ( nextID == null && random <= VISIT_MENTIONED_USER_CUM ) {					
						nextID = jumpUserTweet(currentID, graph.getMentionedUsers(currentID), 1);
						if (nextID != null) logger.debug("JUMP: MN " + currentID + "->" + nextID);
						else random = VISIT_MENTIONED_USER_CUM + rseed.nextDouble()*(1-VISIT_MENTIONED_USER_CUM);
					}

					if ( nextID == null && random <= VISIT_FOLLOWED_USER_CUM ) {	
						Long ownerID = graph.getTweetOwner(currentID);
						if ( ownerID != null) {
							nextID = jumpUserTweet(currentID, graph.getFollowingUsers(ownerID), 0);
							if (nextID != null)	logger.debug("JUMP: FW " + currentID + "->" + nextID);
							else random = VISIT_FOLLOWED_USER_CUM + rseed.nextDouble()*(1-VISIT_FOLLOWED_USER_CUM);
						} 
						else logger.debug("UKN_USER: Unknown user owner of tweet " + currentID);
					}

					if ( nextID == null && random <= VISIT_USED_HASHTAG_CUM ) {
						nextID = jumpHashtagTweet(currentID, graph.getHashtagsByTweet(currentID));
						if (nextID != null) logger.debug("JUMP: HT " + currentID + "->" + nextID);
						else random = VISIT_USED_HASHTAG_CUM + rseed.nextDouble()*(1-VISIT_USED_HASHTAG_CUM);
					}

					if ( nextID == null && random <= VISIT_RANDOM_TWEET_CUM ) {
						nextID = graph.getRandomTweet(rseed);
						logger.debug("JUMP: RND " + currentID + "->" + nextID);
					}

					currentID = nextID;
				} while( rseed.nextDouble() > BORED_PROBABILITY );
			}
		}		


		// Merge & Normalize counters to get the TweetRank approximation
		ArrayList<HashMap<Long,Long>> visitCounters = new ArrayList<HashMap<Long,Long>>();
		visitCounters.add(counter);
		TreeMap<Long,Double> tweetrank = MergeAndNormalizeCounters(visitCounters, 0L, 10L);

		// Work finished!
		StartEndDate[1] = new Date();
		state = State.IDLE;

		logger.info("Ranking finished at " + Time.formatDate("yyyy/MM/dd HH:mm:ss", new Date()));
		return tweetrank;
	}

	/**
	 * This method merges and normalizes a collection of counters. The sum of all the values in the
	 * result HashMap sums 1.0. 
	 * For example, suppose that visitCounters is a collection like { [(1,2), (2,4), (3,1)], [(1,4), (2,3), (4,2)] }
	 * The merged and normalized result would be then [(1,6.0/16.0),(2,7.0/16.0),(3,1.0/16.0),(4,2.0/16.0)]
	 * @param visitCounters Collection of counters to merge and normalize.
	 * @return Returns a merged and normalized HashMap, so that the sum of all values is 1.0.
	 */
	private static TreeMap<Long,Double> MergeAndNormalizeCounters(Collection<HashMap<Long,Long>> visitCounters,
			Long MinRange, Long MaxRange) {
		// Merge all the counters
		TreeMap<Long,Long> merge = new TreeMap<Long,Long>();
		Long sum = 0L;
		Long min = null;
		Long max = null;
		for(HashMap<Long,Long> counter : visitCounters) {
			for(Entry<Long,Long> entry : counter.entrySet()) {
				Long c = merge.get(entry.getKey());
				if ( c == null )  c = entry.getValue();
				else c = c + entry.getValue();
				sum += entry.getValue();
				if ( min == null || min.compareTo(c) > 0 ) min = new Long(c);
				if ( max == null || max.compareTo(c) < 0 ) max = new Long(c);
				merge.put(entry.getKey(), c);
			}
		}

		// check if max and min are equal
		logger.debug("min=" + min + ", max=" + max + ", minRange=" + MinRange + ", maxRange=" + MaxRange);

		// Normalize the counters
		TreeMap<Long,Double> norm = new TreeMap<Long,Double>();
		for(Entry<Long,Long> entry : merge.entrySet()) {
			Double val = MinRange + (MaxRange - MinRange)*(entry.getValue() - min)/(double)(max - min);
			logger.debug("id="+entry.getKey()+", oval=" + entry.getValue() + ", nval=" + val);
			norm.put(entry.getKey(),  val);
			//norm.put(entry.getKey(), entry.getValue()/sum.doubleValue());
		}
		return norm;
	}


	/**
	 * Sets the temporary graph to be used to compute the TweetRank.
	 * @param graph Temporary graph that will be used hereinafter.
	 */
	public void setTemporaryGraph(TemporaryGraph graph) {
		cLock.lock();
		this.graph = graph;
		cLock.unlock();
	}

	/**
	 * Returns the current temporary graph being used. The graph should
	 * not be manipulated with write operations.
	 * @return Number of tweets in the temporary graph.
	 */
	public TemporaryGraph getTemporaryGraph() {
		return graph;
	}	

	/**
	 * Returns the state of the TweetRank computer. WORKING will be returned when the 
	 * computation is active and IDLE when it is not.
	 * WARNING: NOT THREAD-SAFE! 
	 * @return State of the TweetRankComputer.
	 */
	public State getState() {
		return state;
	}

	/**
	 * Returns the elapsed time for the last started TweetRank computation.
	 * WARNING: NOT THREAD-SAFE! 
	 * @return If a computation is ongoing, returns the elapsed time since its beginning,
	 * if the status is IDLE and a computation was completed, returns the elapsed time of
	 * the previous computation, otherwise returns null.
	 */
	public Time getElapsedTime() {
		if ( state == State.WORKING ) {
			return new Time((new Date()).getTime() - StartEndDate[0].getTime());
		} else if ( StartEndDate[1] != null && StartEndDate[1].compareTo(StartEndDate[0]) > 0 ) {
			return new Time(StartEndDate[1].getTime() - StartEndDate[0].getTime());
		} else {
			return null;
		}
	}

	/**
	 * Returns the end date of the last TweetRank computation.
	 * WARNING: NOT THREAD-SAFE! 
	 * @return Date of the last computation.
	 */
	public Date getEndDate() {
		return StartEndDate[1];
	}
}
