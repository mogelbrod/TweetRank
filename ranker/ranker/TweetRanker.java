package ranker;

import graph.TemporaryGraph;

import java.util.*;
import java.util.Map.Entry;

public class TweetRanker {
	/* Cummulative probabilities. TODO: adjust this parameters. */
	private final double VISIT_REFERENCED_TWEET = 0.30; // 30%
	private final double VISIT_MENTIONED_USER   = 0.55; // 25%
	private final double VISIT_FOLLOWED_USER    = 0.65; // 10%
	private final double VISIT_USED_HASHTAG     = 0.90; // 25%
	//private final double RANDOM_JUMP            = 1.00; // 10%

	/** Mapping between a tweet and how many times it's been visited by the surfer */
	private HashMap<Long, Long> tweetVisited = new HashMap<Long, Long>();	

	private TemporaryGraph graph;

	public TweetRanker(TemporaryGraph graph) {
		this.graph = graph;
	}

	public HashMap<Long,Double> computePageRank() {
		int m = graph.getNumberOfTweets()/5;
		return MCCompletePath(m);
	}

	private void addVisit(Long tweetID) {
		Long c = tweetVisited.get(tweetID);
		if ( c == null ) c = 1L;
		else c = c + 1;
		tweetVisited.put(tweetID, c);
	}


	/** Jump to a random tweet from a random related user (mentioned/followed, just pass the appropiate list). */
	private Long jumpUserTweet(ArrayList<Long> usersList, Random r) {
		// If there are no related users, then jump to a random tweet.
		if (usersList == null || usersList.size() == 0) return graph.getRandomTweet(r);

		Long randomUser = usersList.get(r.nextInt(usersList.size()));
		ArrayList<Long> tweetsOfUser = graph.getUserTweets(randomUser);

		// If the related user does not have tweets, we jump to a random tweet.
		if (tweetsOfUser == null || tweetsOfUser.size() == 0) return graph.getRandomTweet(r);

		return tweetsOfUser.get(r.nextInt(tweetsOfUser.size()));
	}

	/** Jump to a random hashtag for the given tweet, and then to a random tweet for that hashtag. */
	private Long jumpHashtagTweet(Long tweetID, Random r) /*throws MegaMapException*/ {
		ArrayList<String> tweet_hts = graph.getHashtagsByTweet(tweetID);
		if (tweet_hts == null || tweet_hts.size() == 0) return graph.getRandomTweet(r);

		String randomHashtag = tweet_hts.get(r.nextInt(tweet_hts.size()));
		ArrayList<Long> hashtag_tws = graph.getTweetsByHashtag(randomHashtag);
		return hashtag_tws.get(r.nextInt(hashtag_tws.size()));
	}

	/** Jump to the referenced tweet. */
	private Long jumpReferenceTweet(Long tweetID, Random r) {
		Long nid = graph.getRefTweet(tweetID);
		if (nid == null) return graph.getRandomTweet(r);
		else return nid;
	}

	private HashMap<Long,Double> MCCompletePath(int m) {
		Random r = new Random();
		double random;

		for(Long currentID : graph.getTweetSet()) {
			for(int i = 1; i <= m; ++i) {
				random = r.nextDouble();

				if ( random <= VISIT_REFERENCED_TWEET )
					currentID = jumpReferenceTweet(currentID, r);
				else if ( random <= VISIT_MENTIONED_USER )
					currentID = jumpUserTweet(graph.getMentionedUsers(currentID), r);
				else if ( random <= VISIT_FOLLOWED_USER )
					currentID = jumpUserTweet(graph.getFollowingUsers(graph.getTweetOwner(currentID)), r);
				else if ( random <= VISIT_USED_HASHTAG )
					currentID = jumpHashtagTweet(currentID, r);
				else
					currentID = graph.getRandomTweet(r);

				addVisit(currentID);
			}
		}

		HashMap<Long,Double> pagerank = new HashMap<Long,Double>();
		for(Entry<Long,Long> entry : tweetVisited.entrySet()) {
			Double tweetRank = entry.getValue()/(double)(graph.getNumberOfTweets() * m);
			pagerank.put(entry.getKey(), tweetRank);
		}
		return pagerank;
	}
}
