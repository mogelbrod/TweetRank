package graph;

import java.util.*;

import org.apache.log4j.Logger;

public class TemporaryGraph {
	private static final Logger logger = Logger.getLogger("ranker.logger");

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

	private static Hashtable<Long,ArrayList<Long>> convertFilteredFriends(Hashtable<Long, HashSet<Long>> friends, 
			Hashtable<Long,ArrayList<Long>> userTweets) {
		Hashtable<Long, ArrayList<Long>> filtered_friends = new Hashtable<Long, ArrayList<Long>>(); 
		for ( Map.Entry<Long, HashSet<Long>> entry : friends.entrySet() ) {
			Long user = entry.getKey(); // Current user
			ArrayList<Long> f_userfriends = new ArrayList<Long>(); // Filtered list of user's friends
			
			// Traverses all the user's friends
			for( Long friend : entry.getValue() ) { 
				// If the friend has posted some tweet, then add the friend to the filtered list of friends
				ArrayList<Long> tweetsByFriend = userTweets.get(friend);
				if ( tweetsByFriend != null && tweetsByFriend.size() > 0 )
					f_userfriends.add(friend);
			}
			
			filtered_friends.put(user, f_userfriends);
		}

		return filtered_friends;
	}


	/** Constructor.  */
	public TemporaryGraph(PersistentGraph sgraph) {
		sgraph.lockAll();
		try {
			tweetSet = new Hashtable<Long,Long>(sgraph.getTweetSet());
			tweetList = new ArrayList<Long>(tweetSet.keySet());
			refTweets = new Hashtable<Long,Long>(sgraph.getRefTweets());
			userTweets = convertHashtable(sgraph.getUserTweets());
			mentioned = convertHashtable(sgraph.getMentioned());
			hashtagsByTweet = convertHashtable(sgraph.getHashtagsByTweet());
			tweetsByHashtag = convertHashtable(sgraph.getTweetsByHashtag());
			follows = convertFilteredFriends(sgraph.getFollows(), userTweets);
		} catch (Throwable t) {
			logger.error("Error creating a new temporary graph.", t);			
		} finally {
			sgraph.unlockAll();
		}
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