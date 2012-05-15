package graph;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FilteredGraph extends Graph<HashSet<Long>, HashSet<String>> {
	private ArrayList<Long> tweetsList;

	@SuppressWarnings("unchecked")
	public FilteredGraph(String path, String prefix, Integer version) throws Throwable {
		tweets = (HashMap<Long, Long>)utils.Functions.loadObject(path, prefix + "__TweetSet-" + version);
		if ( tweets == null ) throw new FileNotFoundException("File \"" + prefix + "__TweetSet-" + version + "\" not found!");
		tweets = filterActiveTweets(tweets);
		tweetsList = new ArrayList<Long>(tweets.keySet());
		
		tweetsByUser  = (HashMap<Long, HashSet<Long>>)utils.Functions.loadObject(path, prefix + "__UserTweets-" + version);
		if ( tweetsByUser == null ) throw new FileNotFoundException("File \"" + prefix + "__UserTweets-" + version + "\" not found!");
		tweetsByUser = filterActiveTweetsByUser(tweetsByUser, tweets.keySet());
		
		references  = (HashMap<Long, Long>)utils.Functions.loadObject(path, prefix + "__RefTweets-" + version);
		if ( references == null ) throw new FileNotFoundException("File \"" + prefix + "__RefTweets-" + version + "\" not found!");
		references = filterActiveReferences(references, tweets.keySet());		

		mentions  = (HashMap<Long, HashSet<Long>>)utils.Functions.loadObject(path, prefix + "__Mention-" + version);
		if ( mentions == null ) throw new FileNotFoundException("File \"" + prefix + "__TweetSet-" + version + "\" not found!");
		mentions = filterActiveMentionship(mentions, tweetsByUser.keySet());

		friends  = (HashMap<Long, HashSet<Long>>)utils.Functions.loadObject(path, prefix + "__Follows-" + version);
		if ( friends == null ) throw new FileNotFoundException("File \"" + prefix + "__TweetSet-" + version + "\" not found!");
		friends = filterActiveFriendship(friends, tweetsByUser.keySet());

		hashtagsByTweet  = (HashMap<Long, HashSet<String>>)utils.Functions.loadObject(path, prefix + "__HashtagsByTweet-" + version);
		if ( hashtagsByTweet == null ) throw new FileNotFoundException("File \"" + prefix + "__HashtagsByTweet-" + version + "\" not found!");
		hashtagsByTweet = filterActiveHashtagsByTweet(hashtagsByTweet, tweets.keySet());

		tweetsByHashtag  = (HashMap<String, HashSet<Long>>)utils.Functions.loadObject(path, prefix + "__TweetsByHashtag-" + version);
		if ( tweetsByHashtag == null ) throw new FileNotFoundException("File \"" + prefix + "__TweetsByHashtag-" + version + "\" not found!");
		tweetsByHashtag = filterActiveTweetsByHashtag(tweetsByHashtag, tweets.keySet());
	}

	@Override
	protected HashSet<Long> filterActiveFriends(HashSet<Long> friends, Set<Long> validUsers) {
		HashSet<Long> activeFriends = new HashSet<Long>();
		for( Long friend : friends )
			if(validUsers.contains(friend))
				activeFriends.add(friend);
		return activeFriends;
	}

	@Override
	protected HashSet<Long> filterActiveMentions(HashSet<Long> mentions, Set<Long> validUsers) {
		HashSet<Long> activeMentions = new HashSet<Long>();
		for( Long user : mentions )
			if(validUsers.contains(user))
				activeMentions.add(user);
		return activeMentions;
	}

	public Set<Long> getTweetSet() {
		return tweets.keySet();
	}

	public List<Long> getTweetsList() {
		return tweetsList;
	}

	@Override
	protected HashSet<Long> filterTweetsCollection(HashSet<Long> tweets, Set<Long> validTweets) {
		return utils.Functions.SetIntersection(tweets, validTweets);
	}
	
	public double percentageTweetsWithHashtag() {
		int TT = 0;
		for( Map.Entry<Long, HashSet<String>> e : hashtagsByTweet.entrySet() ) {
			if ( e.getValue().size() > 0 ) TT++;
		}
		return TT/(double)tweets.size();
	}
}
