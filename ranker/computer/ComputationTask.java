package computer;

import graph.TemporaryGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

public class ComputationTask implements Callable<HashMap<Long,Long>> {
	/* TODO: adjust this parameters. */
	public static final double PROB_VISIT_RANDOM    = 0.20; // 20% 
	public static final double PROB_VISIT_REFERENCE = 0.40; // 40% 
	public static final double PROB_VISIT_MENTIONED = 0.20; // 20%
	public static final double PROB_VISIT_FRIEND    = 0.04; //  4%
	public static final double PROB_VISIT_HASHTAG   = 0.16; // 16%

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
	private Long jumpUserListTweet(Long TweetID, ArrayList<Long> usersList) {
		Long randomUser = usersList.get(rseed.nextInt(usersList.size()));
		ArrayList<Long> tweetsOfUser = graph.getTweetsByUser(randomUser);
		return tweetsOfUser.get(rseed.nextInt(tweetsOfUser.size()));
	}

	/** 
	 * Jumps to a random hashtag for the given tweet, and then to a random tweet for that hashtag. 
	 * @return New tweet id. Returns null if the tweet has no hashtags associated.
	 */
	private Long jumpHashtagTweet(Long TweetID ) {
		ArrayList<String> tweetHashtags = graph.getHashtagsByTweet(TweetID);
		String randomHashtag = tweetHashtags.get(rseed.nextInt(tweetHashtags.size()));
		ArrayList<Long> hashtag_tws = graph.getTweetsByHashtag(randomHashtag);
		return hashtag_tws.get(rseed.nextInt(hashtag_tws.size()));
	}	

	@Override
	public HashMap<Long,Long> call() {		
		List<Long> tweets = graph.getTweetList();

		double [] CUMULATIVE_PROBABILITIES = new double [5];
		CUMULATIVE_PROBABILITIES[0] = PROB_VISIT_RANDOM;
		for(int tweet_i = task_id; tweet_i < tweets.size(); tweet_i += ntasks) {
			for(int i = 1; i <= M; ++i) {
				Long currentID = tweets.get(tweet_i);

				boolean stop_walk = false;
				do {
					CUMULATIVE_PROBABILITIES[1] = CUMULATIVE_PROBABILITIES[0] + (graph.hasReferences(currentID) ? 1: 0)*PROB_VISIT_REFERENCE;
					CUMULATIVE_PROBABILITIES[2] = CUMULATIVE_PROBABILITIES[1] + (graph.hasMentions(currentID) ? 1 : 0)*PROB_VISIT_MENTIONED;
					CUMULATIVE_PROBABILITIES[3] = CUMULATIVE_PROBABILITIES[2] + (graph.hasKnownAuthor(currentID) && 
																				 graph.hasFriends(graph.getTweetAuthor(currentID)) ? 1 : 0)*PROB_VISIT_FRIEND;
					CUMULATIVE_PROBABILITIES[4] = CUMULATIVE_PROBABILITIES[3] + (graph.hasHashtags(currentID) ? 1 : 0)*PROB_VISIT_HASHTAG;

					double random = rseed.nextDouble()*CUMULATIVE_PROBABILITIES[4];

					if ( random <= CUMULATIVE_PROBABILITIES[0] ) { 
						currentID = graph.getTweetList().get(rseed.nextInt(graph.getTweetList().size()));
						stop_walk = true;
					} else if ( random <= CUMULATIVE_PROBABILITIES[1] ) 
						currentID = graph.getReference(currentID);
					else if ( random <= CUMULATIVE_PROBABILITIES[2] )
						currentID = jumpUserListTweet(currentID, graph.getMentions(currentID));
					else if ( random <= CUMULATIVE_PROBABILITIES[3] )
						currentID = jumpUserListTweet(currentID, graph.getFriends(graph.getTweetAuthor(currentID)));
					else
						currentID = jumpHashtagTweet(currentID);

					assert(currentID != null && currentID.compareTo(0L) >= 0);
					addVisit(currentID);
				} while ( !stop_walk );	
			}
		}

		return counter;
	}

	public long getTotalCounter() {
		return total_counter;
	}
}