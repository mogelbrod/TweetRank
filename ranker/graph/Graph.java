package graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class Graph <LongCollection extends Collection<Long>, StringCollection extends Collection<String>> {
	protected HashMap<Long, Long> tweets;
	protected HashMap<Long, Long> references;
	protected HashMap<Long, StringCollection> hashtagsByTweet;
	protected HashMap<String, LongCollection> tweetsByHashtag;
	protected HashMap<Long, LongCollection> tweetsByUser;
	protected HashMap<Long, LongCollection> mentions;
	protected HashMap<Long, LongCollection> friends;

	public Long getReference(Long tweetID) {
		return references.get(tweetID);
	}

	public StringCollection getHashtagsByTweet(Long tweetID) {
		return hashtagsByTweet.get(tweetID);
	}

	public LongCollection getTweetsByHashtag(String hashtag) {
		return tweetsByHashtag.get(hashtag);
	}

	public LongCollection getTweetsByUser(Long userID) {
		return tweetsByUser.get(userID);
	}

	public LongCollection getMentions(Long tweetID) {
		return mentions.get(tweetID);
	}

	public LongCollection getFriends(Long userID) {
		return friends.get(userID);
	}

	public Long getTweetAuthor(Long tweetID) {
		return tweets.get(tweetID);
	}

	protected abstract LongCollection filterActiveFriends(LongCollection friends, HashMap<Long, LongCollection> tweetsByUser);
	protected abstract LongCollection filterActiveMentions(LongCollection mentions, HashMap<Long, LongCollection> tweetsByUser);

	protected HashMap<Long, LongCollection> filterActiveMentionship(HashMap<Long,LongCollection> mentions, HashMap<Long,LongCollection> tweetsByUser) {
		HashMap<Long, LongCollection> activeMentionship = new HashMap<Long, LongCollection>();
		for(Map.Entry<Long, LongCollection> e : mentions.entrySet()) {
			LongCollection activeMentions = filterActiveMentions(e.getValue(), tweetsByUser);
			if ( activeMentions.size() > 0 )
				activeMentionship.put(e.getKey(), activeMentions);
		}
		return activeMentionship;	
	}

	protected HashMap<Long, LongCollection> filterActiveFriendship(HashMap<Long,LongCollection> friends, HashMap<Long,LongCollection> tweetsByUser) {
		HashMap<Long, LongCollection> activeFriendship = new HashMap<Long, LongCollection>();
		for(Map.Entry<Long, LongCollection> e : friends.entrySet()) {
			LongCollection activeFriends = filterActiveFriends(e.getValue(), tweetsByUser);
			if ( activeFriends.size() > 0 )
				activeFriendship.put(e.getKey(), activeFriends);
		}
		return activeFriendship;		
	}

	protected HashMap<Long, Long> filterActiveTweets(HashMap<Long,Long> tweets) {
		HashMap<Long,Long> activeTweets = new HashMap<Long, Long>();
		for(Map.Entry<Long, Long> e : tweets.entrySet()) {
			if ( e.getValue() != null && e.getValue().compareTo(0L) >= 0 )
				activeTweets.put(e.getKey(), e.getValue());
		}
		return activeTweets;
	}
	
	protected HashMap<Long, Long> filterActiveReferences(HashMap<Long,Long> references) {
		HashMap<Long,Long> activeReferences = new HashMap<Long,Long>();
		for(Map.Entry<Long, Long> e : references.entrySet()) {
			if (hasKnownAuthor(e.getKey()) && hasKnownAuthor(e.getValue()))
				activeReferences.put(e.getKey(), e.getValue());
		}
		return activeReferences;
	}

	public boolean hasKnownAuthor(Long tweet) {
		return (tweets.get(tweet) != null && tweets.get(tweet).compareTo(0L) >= 0);
	}

	public boolean hasReferences(Long tweet) {
		return (references.get(tweet) != null );
	}

	public boolean hasMentions(Long tweet) {
		return ( mentions.get(tweet) != null && mentions.get(tweet).size() > 0 );
	}

	public boolean hasHashtags(Long tweet) {
		return ( hashtagsByTweet.get(tweet) != null && hashtagsByTweet.get(tweet).size() > 0);
	}

	public boolean hasFriends(Long user) {
		return ( friends.get(user) != null && friends.get(user).size() > 0);
	}

	public int getNumberOfTweets() {
		return tweets.size();
	}

	public int getNumberOfUsers() {
		return tweetsByUser.size();
	}

	public int getNumberOfHashtags() {
		return tweetsByHashtag.size();
	}

	public double getAverageTweetsPerUser() {
		if (tweetsByUser.size() == 0) return 0.0;
		return tweets.size()/(double)tweetsByUser.size();
	}

	public double getAverageReferencePerTweet() {
		if (tweets.size() == 0) return 0.0;
		return references.size()/(double)tweets.size();
	}

	public double getAverageFriendsPerUser() {
		if (friends.size() == 0) return 0.0;
		int Tfriends = 0;
		for( LongCollection l : friends.values() )
			Tfriends += l.size();
		return Tfriends/(double)friends.size();
	}

	public double getAverageMentionsPerTweet() {
		if(tweets.size() == 0) return 0.0;
		int Tmentions = 0;
		for( LongCollection l : mentions.values() )
			Tmentions += l.size();
		return Tmentions/(double)tweets.size();
	}

	public double getAverageHashtagsPerTweet() {
		if ( tweets.size() == 0 ) return 0.0;
		int Thashtags = 0;
		for( StringCollection l : hashtagsByTweet.values() )
			Thashtags += l.size();
		return Thashtags/(double)tweets.size();
	}

	public abstract double getAverageActiveFriendsPerUser();

	public abstract double getAverageActiveMentionsPerTweet();

	//public abstract double getAverageActiveHashtagsPerTweet();
}
