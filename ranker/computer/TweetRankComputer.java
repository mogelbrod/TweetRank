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
	private static final double VISIT_REFERENCED_TWEET_CUM = 0.40; // 40% 
	private static final double VISIT_MENTIONED_USER_CUM   = 0.60; // 20%
	private static final double VISIT_FOLLOWED_USER_CUM    = 0.64; //  4%
	private static final double VISIT_USED_HASHTAG_CUM     = 0.80; // 16%
	private static final double BORED_PROBABILITY_CUM      = 1.00; // 20%

	private static final int NUM_WORK_THREADS = 16;
	ComputerThread[] workerThreads = new ComputerThread[NUM_WORK_THREADS];	

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

	private class ComputerThread extends Thread {
		private int tidx;
		private HashMap<Long,Long> counter;
		private long total_counter;
		private Random rseed = new Random();

		public ComputerThread(int tidx) {
			super();
			this.tidx = tidx;
		}

		/**
		 * Adds a visit to the specified tweetID. 
		 * This method is thread-safe, multiple threads can add visits concurrently.
		 * @param tweetID Visited tweet id.
		 */
		private void addVisit(Long tweetID) {
			Long c = counter.get(tweetID);
			if ( c == null ) counter.put(tweetID, 1L);
			else counter.put(tweetID, c+1);
			total_counter++;
		}

		/** 
		 * Jumps to a random tweet from a random related user (mentioned/followed, just pass the appropiate list).
		 * @param userList List of users to select a random user.
		 * @return New tweet id. Returns null if the passed list is empty or the selected random user has no tweets.
		 */
		private Long jumpUserTweet(Long TweetID, ArrayList<Long> usersList, long type) {
			// If there are no related users, then jump to a random tweet.
			if (usersList == null || usersList.size() == 0) {
				if (type == 0) logger.debug("EMPTY_UL (FW): Owner of tweet " + TweetID.toString() + " has no friends.");
				else logger.debug("EMPTY_UL (MN): Tweet " + TweetID.toString() + "has no mentions.");
				return null;
			}

			Long randomUser = usersList.get(rseed.nextInt(usersList.size()));
			ArrayList<Long> tweetsOfUser = graph.getUserTweets(randomUser);

			// If the related user does not have tweets, we jump to a random tweet.
			if (tweetsOfUser == null || tweetsOfUser.size() == 0) {
				logger.debug("SILENT_USER: User " + randomUser.toString() + " has no tweets.");
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
				logger.debug("EMPTY_HT: Tweet " + TweetID.toString() + " has no hashtags.");
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

		@Override
		public void run() {
			counter = new HashMap<Long, Long> ();
			total_counter = 0;
			
			List<Long> tweets = graph.getTweetList();
			for(int idx = tidx; idx < tweets.size(); idx += NUM_WORK_THREADS) {
				for(int i = 1; i <= M; ++i) {
					Long currentID = tweets.get(idx);

					boolean stop_walk = false;
					do {
						double random = rseed.nextDouble();
						addVisit(currentID);
						Long nextID = null;

						if ( nextID == null && random <= VISIT_REFERENCED_TWEET_CUM ) {
							nextID = jumpReferenceTweet(currentID);
							if (nextID != null) logger.debug("JUMP: REF " + currentID.toString() + "->" + nextID.toString());
							else random = VISIT_REFERENCED_TWEET_CUM + rseed.nextDouble()*(1-VISIT_REFERENCED_TWEET_CUM);
						}

						if ( nextID == null && random <= VISIT_MENTIONED_USER_CUM ) {					
							nextID = jumpUserTweet(currentID, graph.getMentionedUsers(currentID), 1);
							if (nextID != null) logger.debug("JUMP: MN " + currentID.toString() + "->" + nextID.toString());
							else random = VISIT_MENTIONED_USER_CUM + rseed.nextDouble()*(1-VISIT_MENTIONED_USER_CUM);
						}

						if ( nextID == null && random <= VISIT_FOLLOWED_USER_CUM ) {	
							Long ownerID = graph.getTweetOwner(currentID);
							if ( ownerID != null) {
								nextID = jumpUserTweet(currentID, graph.getFollowingUsers(ownerID), 0);
								if (nextID != null)	logger.debug("JUMP: FW " + currentID.toString() + "->" + nextID.toString());
								else random = VISIT_FOLLOWED_USER_CUM + rseed.nextDouble()*(1-VISIT_FOLLOWED_USER_CUM);
							} 
							else logger.debug("UKN_USER: Unknown user owner of tweet " + currentID.toString());
						}

						if ( nextID == null && random <= VISIT_USED_HASHTAG_CUM ) {
							nextID = jumpHashtagTweet(currentID, graph.getHashtagsByTweet(currentID));
							if (nextID != null) logger.debug("JUMP: HT " + currentID.toString() + "->" + nextID.toString());
							else random = VISIT_USED_HASHTAG_CUM + rseed.nextDouble()*(1-VISIT_USED_HASHTAG_CUM);
						}

						/* We reached a dangling node. Then, jump to a random tweet, and stop the current random walk. */
						if ( nextID == null && random <= BORED_PROBABILITY_CUM ) {
							nextID = graph.getRandomTweet(rseed);
							logger.debug("JUMP: RND " + currentID.toString() + "->" + nextID.toString());
							stop_walk = true;
						}

						currentID = nextID;
					} while ( !stop_walk );
				}
			}
		}

		public HashMap<Long,Long> getCounter() {
			return counter;
		}

		public long getTotalCounter() {
			return total_counter;
		}
	}


	/**
	 * Starts the computation of the TweetRank.
	 * @return A HashMap where each entry is a pair (TweetID, TweetRank). If any problem
	 * ocurred, the result is null.
	 * @throws ConcurrentComputationException is thrown if there is a concurrent computation.
	 * @throws NullTemporaryGraphException if the graph was not initialized.
	 */
	public TreeMap<Long,Double> compute(TemporaryGraph graph) 
	throws ConcurrentComputationException, NullTemporaryGraphException 
	{
		TreeMap<Long,Double> tweetrank = null;

		// Check if there is another thread already computing the tweetrank
		if ( !cLock.tryLock() ) 
			throw new ConcurrentComputationException();

		// Check if the graph is null
		if ( graph == null ) {
			cLock.unlock();
			throw new NullTemporaryGraphException();
		}
			
		try {
			// Set the temporary graph
			this.graph = graph;
			
			// Determine the path length to be used
			M = graph.getNumberOfTweets()/100;
			if (M < 100) M = 100;

			// Start computation!
			tweetrank = MCCompletePathStopDanglingNodes();
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
	private TreeMap<Long,Double> MCCompletePathStopDanglingNodes() {	
		logger.info("Ranking started at " + Time.formatDate("yyyy/MM/dd HH:mm:ss", new Date()));

		// Work started...
		state = State.WORKING;
		StartEndDate[0] = new Date();		

		// Start workers
		for(int widx = 0; widx < NUM_WORK_THREADS; ++widx) {
			workerThreads[widx] = new ComputerThread(widx);
			workerThreads[widx].start();
		}

		// Wait until all the workers have finished
		boolean interrupted = false;
		for(int widx = 0; widx < NUM_WORK_THREADS; ++widx) {
			try 
			{ workerThreads[widx].join();	} 
			catch (InterruptedException e)
			{ logger.info("Interrupted thread", e); interrupted = true; }
		}

		// If any worker was interrupted, discard the computation.
		if (interrupted) return null;

		// Merge & Normalize counters to get the TweetRank approximation
		ArrayList<HashMap<Long,Long>> visitCounters = new ArrayList<HashMap<Long,Long>>();
		for(int widx = 0; widx < NUM_WORK_THREADS; ++widx) 
			visitCounters.add(workerThreads[widx].getCounter());
		TreeMap<Long,Double> tweetrank = MergeAndNormalizeCounters(visitCounters, 0L, 10L);

		// Force the destruction of worker threads.
		for(int widx = 0; widx < NUM_WORK_THREADS; ++widx)
			workerThreads[widx] = null;

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


		logger.debug("min=" + min.toString() + ", max=" + max.toString() + ", minRange=" + MinRange.toString() + ", maxRange=" + MaxRange.toString());

		// Normalize the counters
		TreeMap<Long,Double> norm = new TreeMap<Long,Double>();

		// Check if max and min are equal
		if ( !max.equals(min) ) {
			for(Entry<Long,Long> entry : merge.entrySet()) {
				Double val = MinRange + (MaxRange - MinRange)*(entry.getValue() - min)/(double)(max - min);
				logger.debug("id="+entry.getKey()+", oval=" + entry.getValue() + ", nval=" + val);
				norm.put(entry.getKey(),  val);
			}			
		} else {
			for(Entry<Long,Long> entry : merge.entrySet()) {
				Double val = MinRange + (MaxRange - MinRange)/2.0;
				logger.debug("id="+entry.getKey()+", oval=" + entry.getValue() + ", nval=" + val);
				norm.put(entry.getKey(), val);
			}
		}

		return norm;
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
	
	/**
	 * Returns the percentage of completion of the TweetRank computation.
	 * WARNING: NOT THREAD-SAFE! 
	 * @return If the computation is active, returns the percentage of completion of the TweetRank computation.
	 * Otherwise returns 0.
	 */
	public double getExpectedPercentageOfCompletion() {
		if ( state == State.IDLE ) return 0.0;
		
		double bored_prob = BORED_PROBABILITY_CUM - VISIT_USED_HASHTAG_CUM;
		double expected_length = 1/bored_prob; // Expected length
		double dev_length = Math.sqrt(3.0)/6.0;// Standard deviation in the length

		double ExpectedVisits = graph.getNumberOfTweets()*M*expected_length;
		long CurrentVisits = 0L;

		for(int widx = 0; widx < NUM_WORK_THREADS; ++widx)
			CurrentVisits += workerThreads[widx].getTotalCounter();
		
		while(CurrentVisits > ExpectedVisits)
			ExpectedVisits += dev_length;
		
		return CurrentVisits/ExpectedVisits;
	}
	
	/**
	 * Returns the expected remaining time for the completion of the ongoing
	 * TweetRank computation (or zero, if state is IDLE).
	 * WARNING: NOT THREAD-SAFE! 
	 * @return Expected remaining time.
	 */
	public Time getExpectedRemainingTime() {
		if ( state == State.IDLE ) return new Time(0);

		double completed = getExpectedPercentageOfCompletion();
		if (completed < 1E-5) return new Time();

		long elapsed = (new Date()).getTime() - StartEndDate[0].getTime();
		return new Time((long)(elapsed/completed) - elapsed);
	}	
}
