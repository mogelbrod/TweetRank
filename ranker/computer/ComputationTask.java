package computer;

import graph.TemporaryGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

public class ComputationTask implements Callable<HashMap<Long,Long>> {
	private static final Logger logger = Logger.getLogger("ranker.logger");
	
	/* TODO: adjust this parameters. */
	public static final double VISIT_RANDOM_TWEET_CUM     = 0.20; // 20% 
	public static final double VISIT_REFERENCED_TWEET_CUM = 0.60; // 40% 
	public static final double VISIT_MENTIONED_USER_CUM   = 0.80; // 20%
	public static final double VISIT_FOLLOWED_USER_CUM    = 0.84; //  4%
	public static final double VISIT_USED_HASHTAG_CUM     = 1.00; // 16%
	
	public static final double BORED_PROBABILITY          = 0.2; // 20%
	
	private TemporaryGraph graph;
	private int task_id;
	private int ntasks;
	private int M;
	
	private HashMap<Long,Long> counter = new HashMap<Long, Long> ();
	private long total_counter = 0L;
	private Random rseed = new Random();

	public ComputationTask(int id, int ntasks, int M, TemporaryGraph graph) {
		super();
		this.task_id = id;
		this.ntasks  = ntasks;
		this.M = M;
		this.graph = graph;
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
	public HashMap<Long,Long> call() {		
		List<Long> tweets = graph.getTweetList();
		
		for(int tweet_i = task_id; tweet_i < tweets.size(); tweet_i += ntasks) {
			for(int i = 1; i <= M; ++i) {
				Long currentID = tweets.get(tweet_i);

				boolean stop_walk = false;
				do {
					double random = rseed.nextDouble();
					addVisit(currentID);
					Long nextID = null;
					
					if ( random <= VISIT_RANDOM_TWEET_CUM ) {
						nextID = graph.getRandomTweet(rseed);
						logger.debug("JUMP: RND " + currentID.toString() + "->" + nextID.toString());						
					}

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
					if ( nextID == null ) stop_walk = true;
					else stop_walk = (rseed.nextDouble() <  BORED_PROBABILITY);

					currentID = nextID;
				} while ( !stop_walk );	
			}
		}

		return counter;
	}

	public long getTotalCounter() {
		return total_counter;
	}
}