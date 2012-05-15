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

	public double getAverageActiveFriendsPerUser() {
		return getAverageFriendsPerUser();
	}

	public double getAverageActiveMentionsPerTweet() {
		return getAverageMentionsPerTweet();
	}

	@Override
	protected ArrayList<Long> filterActiveFriends(ArrayList<Long> friends, Set<Long> validUsers) {
		ArrayList<Long> activeFriends = new ArrayList<Long>();
		for( Long friend : friends )
			if ( validUsers.contains(friend) )
				activeFriends.add(friend);
		return activeFriends;
	}

	@Override
	protected ArrayList<Long> filterActiveMentions(ArrayList<Long> mentions, Set<Long> validUsers) {
		ArrayList<Long> activeMentions = new ArrayList<Long>();
		for( Long user : mentions )
			if ( validUsers.contains(user) )
				activeMentions.add(user);
		return activeMentions;
	}

	@Override
	protected ArrayList<Long> filterTweetsCollection(ArrayList<Long> tweets, Set<Long> validTweets) {
		// TODO Auto-generated method stub
		return null;
	}
}