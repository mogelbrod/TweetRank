package graph;

import java.util.*;

public class TemporaryGraph {
	/** Contains all tweets */
	private Hashtable<Long,Long> tweetSet;
	private ArrayList<Long> tweetList;

	/** Maps users to tweets */
	private Hashtable<Long,ArrayList<Long>> userTweets;

	/** Maps a tweet to a list of user mentions */
	private Hashtable<Long,ArrayList<Long>> mentioned;

	/** Maps a user to a list of users he/she follows */
	private Hashtable<Long,ArrayList<Long>> follows;

	/** Map a reply/retweet to the original tweet */
	private Hashtable<Long,Long> refTweets;

	/** Map a tweet to a list of hashtags */
	private Hashtable<Long,ArrayList<String>> hashtagsByTweet;
	private Hashtable<String,ArrayList<Long>> tweetsByHashtag;
	
	private static <K,V> Hashtable<K,ArrayList<V>> convertHashtable(Hashtable<K, HashSet<V>> in) {
		Hashtable<K,ArrayList<V>> out = new Hashtable<K,ArrayList<V>>();
		for ( Map.Entry<K, HashSet<V>> entry : in.entrySet() ) {
			out.put(entry.getKey(), new ArrayList<V>(entry.getValue()));
		}
		return out;
	}

	
	/** Constructor.  */
	public TemporaryGraph(PersistentGraph sgraph) {
		tweetSet = new Hashtable<Long,Long>(sgraph.getTweetSet());
		tweetList = new ArrayList<Long>(tweetSet.keySet());
		refTweets = new Hashtable<Long,Long>(sgraph.getRefTweets());
		userTweets = convertHashtable(sgraph.getUserTweets());
		mentioned = convertHashtable(sgraph.getMentioned());
		follows = convertHashtable(sgraph.getFollows());
		hashtagsByTweet = convertHashtable(sgraph.getHashtagsByTweet());
		tweetsByHashtag = convertHashtable(sgraph.getTweetsByHashtag());
	}
	
	public Long getRandomTweet(Random r) {
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
		return mentioned.get(tweetID);
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
	
	public double getAverageFriendsPerUser() {
		if (follows.size() == 0) return 0.0;
		
		int Tfollowed = 0;
		for( ArrayList<Long> l : follows.values() )
			Tfollowed += l.size();
		return Tfollowed/follows.size();
	}
	
	public double getAverageMentionsPerTweet() {
		if( tweetSet.size() == 0 ) return 0.0;

		int Tmentions = 0;
		for( ArrayList<Long> l : mentioned.values() )
			Tmentions += l.size();
		return Tmentions/(double)tweetSet.size();
	}
}