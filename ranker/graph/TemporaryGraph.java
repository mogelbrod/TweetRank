package graph;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
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
	private HashMap<Long,ArrayList<Long>> mentions;

	/** Maps a user to a list of users he/she follows */
	private HashMap<Long,ArrayList<Long>> follows;

	/** Map a reply/retweet to the original tweet */
	private HashMap<Long,Long> refTweets;

	/** Map a tweet to a list of hashtags */
	private HashMap<Long,ArrayList<String>> hashtagsByTweet;
	private HashMap<String,ArrayList<Long>> tweetsByHashtag;

	private static Object loadObject(String name) {
		Object robject = null;
		try {
			FileInputStream file = new FileInputStream(name);
			ObjectInputStream obj = new ObjectInputStream(file);
			robject = obj.readObject();
		} catch (Throwable t) {
			logger.fatal("Error loading the persistent file "+name, t);
		}
		return robject;
	}


	private static HashMap<Long,ArrayList<Long>> loadLongToArrayLong(String name) {
		@SuppressWarnings("unchecked")
		HashMap<Long,HashSet<Long>> objDisk = (HashMap<Long, HashSet<Long>>)loadObject(name);
		if (objDisk == null) return null;

		HashMap<Long,ArrayList<Long>> out = new HashMap<Long,ArrayList<Long>>();
		for ( Map.Entry<Long, HashSet<Long>> entry : objDisk.entrySet() ) {
			out.put(entry.getKey(), new ArrayList<Long>(entry.getValue()));
		}

		return out;		
	}

	private static HashMap<Long,ArrayList<String>> loadLongToArrayString(String name) {
		@SuppressWarnings("unchecked")
		HashMap<Long,HashSet<String>> objDisk = (HashMap<Long, HashSet<String>>)loadObject(name);
		if (objDisk == null) return null;

		HashMap<Long,ArrayList<String>> out = new HashMap<Long,ArrayList<String>>();
		for ( Map.Entry<Long, HashSet<String>> entry : objDisk.entrySet() ) {
			out.put(entry.getKey(), new ArrayList<String>(entry.getValue()));
		}

		return out;
	}

	private static HashMap<String,ArrayList<Long>> loadStringToArrayLong(String name) {
		@SuppressWarnings("unchecked")
		HashMap<String,HashSet<Long>> objDisk = (HashMap<String, HashSet<Long>>)loadObject(name);
		if (objDisk == null) return null;

		HashMap<String,ArrayList<Long>> out = new HashMap<String,ArrayList<Long>>();
		for ( Map.Entry<String, HashSet<Long>> entry : objDisk.entrySet() ) {
			out.put(entry.getKey(), new ArrayList<Long>(entry.getValue()));
		}

		return out;		
	}

	private HashMap<Long, ArrayList<Long>> loadFriends(String fname, String ufname) {
		@SuppressWarnings("unchecked")
		HashMap<Long,HashSet<Long>> friends = (HashMap<Long, HashSet<Long>>)loadObject(fname);
		if (friends == null) return null;

		@SuppressWarnings("unchecked")
		HashMap<Long,HashSet<Long>> uTweets = (HashMap<Long, HashSet<Long>>)loadObject(ufname);
		if (uTweets == null) return null;

		HashMap<Long, ArrayList<Long>> filtered_friends = new HashMap<Long, ArrayList<Long>>(); 
		for ( Map.Entry<Long, HashSet<Long>> entry : friends.entrySet() ) {
			Long user = entry.getKey(); // Current user
			ArrayList<Long> f_userfriends = new ArrayList<Long>(); // Filtered list of user's friends

			// Traverses all the user's friends
			for( Long friend : entry.getValue() ) { 
				// If the friend has posted some tweet, then add the friend to the filtered list of friends
				HashSet<Long> tweetsByFriend = uTweets.get(friend);
				if ( tweetsByFriend != null && tweetsByFriend.size() > 0 )
					f_userfriends.add(friend);
			}

			filtered_friends.put(user, f_userfriends);
		}

		return filtered_friends;		
	}


	@SuppressWarnings("unchecked")
	public TemporaryGraph(String path, String name, Integer version) {
		mentions = loadLongToArrayLong(path + "/" + name + "__Mention-" + version );
		userTweets = loadLongToArrayLong(path + "/" + name + "__UserTweets-" + version );
		hashtagsByTweet = loadLongToArrayString(path + "/" + name + "__HashtagsByTweet-" + version);
		tweetsByHashtag = loadStringToArrayLong(path + "/" + name + "__TweetsByHashtag-" + version);
		tweetSet = (HashMap<Long, Long>) loadObject(path + "/" + name + "__TweetSet-" + version);
		tweetList = new ArrayList<Long>(tweetSet.keySet());
		refTweets = (HashMap<Long, Long>) loadObject(path + "/" + name + "__RefTweets-" + version);
		follows = loadFriends(path + "/" + name + "__Follows-" + version, path + "/" + name + "__UserTweets-" + version);
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