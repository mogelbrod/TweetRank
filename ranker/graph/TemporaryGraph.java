package graph;

import java.util.*;

public class TemporaryGraph {
	/** Contains all tweets */
	private HashMap<Long,Long> tweetSet;
	private ArrayList<Long> tweetList;

	/** Maps users to tweets */
	private HashMap<Long,ArrayList<Long>> userTweets;

	/** Maps a tweet to a list of user mentions */
	private HashMap<Long,ArrayList<Long>> mentions;

	/** Maps a user to a list of users he/she follows */
	private HashMap<Long,ArrayList<Long>> follows;

	/** Map a reply/retweet to the original tweet */
	private HashMap<Long,Long> refTweets;

	/** Map a tweet to a list of hashtags */
	private HashMap<Long,ArrayList<String>> hashtagsByTweet;
	private HashMap<String,ArrayList<Long>> tweetsByHashtag;

	public TemporaryGraph(HashMap<Long, Long> tTweetSet, HashMap<Long, Long> tRefTweets, 
			HashMap<Long, ArrayList<Long>> tUserTweets,	HashMap<Long, ArrayList<Long>> tMentioned,
			HashMap<Long, ArrayList<Long>> tFollows, HashMap<Long, ArrayList<String>> tHashtagsByTweet,
			HashMap<String, ArrayList<Long>> tTweetsByHashtag) 
	{
		tweetSet = tTweetSet;
		refTweets = tRefTweets;
		userTweets = tUserTweets;
		mentions = tMentioned;
		follows = tFollows;
		hashtagsByTweet = tHashtagsByTweet;
		tweetsByHashtag = tTweetsByHashtag;
		tweetList = new ArrayList<Long>(tweetSet.keySet());
	}

	public Long getRandomTweet(Random r) {
		if (tweetList.size() == 0) return null;
		return tweetList.get(r.nextInt(tweetList.size()));
	}

	public Long getRefTweet(Long tweetID) {
		if ( tweetID == null ) return null;
		return refTweets.get(tweetID);
	}	

	public ArrayList<String> getHashtagsByTweet(Long tweetID) {
		if ( tweetID == null ) return null;
		return hashtagsByTweet.get(tweetID);
	}

	public ArrayList<Long> getTweetsByHashtag(String hashtag) {
		if ( hashtag == null ) return null;
		return tweetsByHashtag.get(hashtag);
	}	

	public ArrayList<Long> getUserTweets(Long userID) {
		if ( userID == null ) return null;
		return userTweets.get(userID);
	}

	public ArrayList<Long> getMentionedUsers(Long tweetID) {
		if ( tweetID == null ) return null;
		return mentions.get(tweetID);
	}

	public ArrayList<Long> getFollowingUsers(Long userID) {
		if ( userID == null ) return null;
		return follows.get(userID);	
	}

	public Long getTweetOwner(Long tweetID) {
		if ( tweetID == null ) return null;
		return tweetSet.get(tweetID);
	}

	public Set<Long> getTweetSet() {
		return tweetSet.keySet();
	}

	public List<Long> getTweetList() {
		return tweetList;
	}	

	public int getNumberOfTweets() {
		return tweetSet.size();
	}

	public int getNumberOfUsers() {
		return userTweets.size();
	}

	public int getNumberOfHashtags() {
		return tweetsByHashtag.size();
	}

	public double getAverageTweetsPerUser() {
		if (userTweets.size() == 0) return 0.0;
		return getNumberOfTweets()/(double)userTweets.size();
	}

	public double getAverageReferencePerTweet() {
		if (tweetSet.size() == 0) return 0.0;
		return refTweets.size()/(double)tweetSet.size();
	}	

	/**
	 * The Effective Friends are those friends who have posted
	 * some tweet. This method returns the mean of the
	 * effective friends that users have.
	 * @return The average number of effective friends per user. 
	 */
	public double getAverageEffectiveFriendsPerUser() {
		if (follows.size() == 0) return 0.0;

		int Tfollowed = 0;
		for( ArrayList<Long> l : follows.values() )
			Tfollowed += l.size();
		return Tfollowed/(double)follows.size();
	}

	public double getAverageMentionsPerTweet() {
		if( tweetSet.size() == 0 ) return 0.0;

		int Tmentions = 0;
		for( ArrayList<Long> l : mentions.values() )
			Tmentions += l.size();
		return Tmentions/(double)tweetSet.size();
	}

	public double getAverageHashtagsPerTweet() {
		if ( tweetSet.size() == 0 ) return 0.0;

		int Thashtags = 0;
		for( ArrayList<String> l : hashtagsByTweet.values() )
			Thashtags += l.size();
		return Thashtags/(double)tweetSet.size();
	}
}