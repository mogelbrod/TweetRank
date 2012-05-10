package graph;

import java.util.*;

import org.apache.log4j.Logger;

public class TemporaryGraph {
	private static final Logger logger = Logger.getLogger("ranker.logger");

	/** Contains all tweets */
	private HashMap<Long,Long> tweetSet;
	private ArrayList<Long> tweetList;

	/** Maps users to tweets */
	private HashMap<Long,ArrayList<Long>> userTweets;

	/** Maps a tweet to a list of user mentions */
	private HashMap<Long,ArrayList<Long>> mentioned;

	/** Maps a user to a list of users he/she follows */
	private HashMap<Long,ArrayList<Long>> follows;

	/** Map a reply/retweet to the original tweet */
	private HashMap<Long,Long> refTweets;

	/** Map a tweet to a list of hashtags */
	private HashMap<Long,ArrayList<String>> hashtagsByTweet;
	private HashMap<String,ArrayList<Long>> tweetsByHashtag;

	private static <K,V> HashMap<K,ArrayList<V>> convertHashMap(HashMap<K, HashSet<V>> in) {
		HashMap<K,ArrayList<V>> out = new HashMap<K,ArrayList<V>>();
		for ( Map.Entry<K, HashSet<V>> entry : in.entrySet() ) {
			out.put(entry.getKey(), new ArrayList<V>(entry.getValue()));
		}
		return out;
	}

	private static HashMap<Long,ArrayList<Long>> convertFilteredFriends(HashMap<Long, HashSet<Long>> friends, 
			HashMap<Long,ArrayList<Long>> userTweets) {
		HashMap<Long, ArrayList<Long>> filtered_friends = new HashMap<Long, ArrayList<Long>>(); 
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
			tweetSet = new HashMap<Long,Long>(sgraph.getTweetSet());
			tweetList = new ArrayList<Long>(tweetSet.keySet());
			refTweets = new HashMap<Long,Long>(sgraph.getRefTweets());
			userTweets = convertHashMap(sgraph.getUserTweets());
			mentioned = convertHashMap(sgraph.getMentioned());
			hashtagsByTweet = convertHashMap(sgraph.getHashtagsByTweet());
			tweetsByHashtag = convertHashMap(sgraph.getTweetsByHashtag());
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
		for( ArrayList<Long> l : mentioned.values() )
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