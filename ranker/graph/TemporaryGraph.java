package graph;

import java.util.*;

public class TemporaryGraph extends Graph <ArrayList<Long>, ArrayList<String>> {
	/** Contains all tweets */
	ArrayList<Long> tweetList;

	public TemporaryGraph(HashMap<Long, Long> tTweetSet, HashMap<Long, Long> tRefTweets, 
			HashMap<Long, ArrayList<Long>> tUserTweets,	HashMap<Long, ArrayList<Long>> tMentioned,
			HashMap<Long, ArrayList<Long>> tFollows, HashMap<Long, ArrayList<String>> tHashtagsByTweet,
			HashMap<String, ArrayList<Long>> tTweetsByHashtag) 
	{
		tweets = tTweetSet;
		references = tRefTweets;
		mentions = tMentioned;
		friends = tFollows;
		tweetsByUser = tUserTweets;
		hashtagsByTweet = tHashtagsByTweet;
		tweetsByHashtag = tTweetsByHashtag;
		tweetList = new ArrayList<Long>(tweets.keySet());
	}

	public Set<Long> getTweetSet() {
		return tweets.keySet();
	}

	public List<Long> getTweetList() {
		return tweetList;
	}

	@Override
	public double getAverageActiveFriendsPerUser() {
		return getAverageFriendsPerUser();
	}

	@Override
	public double getAverageActiveMentionsPerTweet() {
		return getAverageMentionsPerTweet();
	}

	@Override
	protected ArrayList<Long> filterActiveFriends(ArrayList<Long> friends, HashMap<Long, ArrayList<Long>> tweetsByUser) {
		ArrayList<Long> activeFriends = new ArrayList<Long>();
		for( Long friend : friends )
			if ( tweetsByUser.get(friend) != null && tweetsByUser.get(friend).size() > 0 )
				activeFriends.add(friend);
		return activeFriends;
	}

	@Override
	protected ArrayList<Long> filterActiveMentions(ArrayList<Long> mentions, HashMap<Long, ArrayList<Long>> tweetsByUser) {
		ArrayList<Long> activeMentions = new ArrayList<Long>();
		for( Long user : mentions )
			if ( tweetsByUser.get(user) != null && tweetsByUser.get(user).size() > 0 )
				activeMentions.add(user);
		return activeMentions;
	}
}