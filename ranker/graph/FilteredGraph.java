package graph;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FilteredGraph extends Graph<HashSet<Long>, HashSet<String>> {
	private ArrayList<Long> tweetsList;

	@SuppressWarnings("unchecked")
	public FilteredGraph(String path, String prefix, Integer version) throws Throwable {
		tweets = (HashMap<Long, Long>)utils.Functions.loadObject(path, prefix + "__TweetSet-" + version);
		if ( tweets == null ) throw new FileNotFoundException("File \"" + prefix + "__TweetSet-" + version + "\" not found!");
		tweetsList = new ArrayList<Long>(tweets.keySet());
		
		tweetsByUser  = (HashMap<Long, HashSet<Long>>)utils.Functions.loadObject(path, prefix + "__UserTweets-" + version);
		if ( tweetsByUser == null ) throw new FileNotFoundException("File \"" + prefix + "__UserTweets-" + version + "\" not found!");		

		mentions  = (HashMap<Long, HashSet<Long>>)utils.Functions.loadObject(path, prefix + "__Mention-" + version);
		if ( mentions == null ) throw new FileNotFoundException("File \"" + prefix + "__TweetSet-" + version + "\" not found!");
		mentions = filterActiveMentionship(mentions, tweetsByUser);

		friends  = (HashMap<Long, HashSet<Long>>)utils.Functions.loadObject(path, prefix + "__Follows-" + version);
		if ( friends == null ) throw new FileNotFoundException("File \"" + prefix + "__TweetSet-" + version + "\" not found!");
		friends = filterActiveFriendship(friends, tweetsByUser);

		references  = (HashMap<Long, Long>)utils.Functions.loadObject(path, prefix + "__RefTweets-" + version);
		if ( references == null ) throw new FileNotFoundException("File \"" + prefix + "__RefTweets-" + version + "\" not found!");

		hashtagsByTweet  = (HashMap<Long, HashSet<String>>)utils.Functions.loadObject(path, prefix + "__HashtagsByTweet-" + version);
		if ( hashtagsByTweet == null ) throw new FileNotFoundException("File \"" + prefix + "__HashtagsByTweet-" + version + "\" not found!");

		tweetsByHashtag  = (HashMap<String, HashSet<Long>>)utils.Functions.loadObject(path, prefix + "__TweetsByHashtag-" + version);
		if ( tweetsByHashtag == null ) throw new FileNotFoundException("File \"" + prefix + "__TweetsByHashtag-" + version + "\" not found!");
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
	protected HashSet<Long> filterActiveFriends(HashSet<Long> friends, HashMap<Long, HashSet<Long>> tweetsByUser) {
		HashSet<Long> activeFriends = new HashSet<Long>();
		for( Long friend : friends )
			if ( tweetsByUser.get(friend) != null && tweetsByUser.get(friend).size() > 0 )
				activeFriends.add(friend);
		return activeFriends;
	}

	@Override
	protected HashSet<Long> filterActiveMentions(HashSet<Long> mentions, HashMap<Long, HashSet<Long>> tweetsByUser) {
		HashSet<Long> activeMentions = new HashSet<Long>();
		for( Long user : mentions )
			if ( tweetsByUser.get(user) != null && tweetsByUser.get(user).size() > 0 )
				activeMentions.add(user);
		return activeMentions;
	}

	public Set<Long> getTweetSet() {
		return tweets.keySet();
	}

	public List<Long> getTweetsList() {
		return tweetsList;
	}	
}
