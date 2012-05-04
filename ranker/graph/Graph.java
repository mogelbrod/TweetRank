package graph;

import java.io.*;
import java.util.*;

public class Graph {
	/** Contains all tweets */
	private Hashtable<Long,Long> tweetSet;
	private ArrayList<Long> tweetList;

	/** Maps users to tweets */
	private Hashtable<Long,HashSet<Long>> userTweets;

	/** Maps a tweet to a list of user mentions */
	private Hashtable<Long,HashSet<Long>> mentioned;

	/** Maps a user to a list of users he/she follows */
	private Hashtable<Long,HashSet<Long>> follows;

	/** Map a reply/retweet to the original tweet */
	private Hashtable<Long,Long> refTweets;

	/** Map a tweet to a list of hashtags */
	private Hashtable<Long,HashSet<String>> hashtagsByTweet;
	private Hashtable<String,HashSet<Long>> tweetsByHashtag;

	private String name;
	private String path;
	
	private static Object loadObject(String path, String name) {
		try {
			FileInputStream file = new FileInputStream(path + "/" + name);
			ObjectInputStream obj = new ObjectInputStream(file);
			return obj.readObject();
		} catch ( Exception e ) {
			return null;
		}
	}
	
	private static void saveObject(String path, String name, Object obj) {
		try {
			FileOutputStream fObject   = new FileOutputStream(path + "/" + name);
			ObjectOutputStream oObject = new ObjectOutputStream(fObject);
			oObject.writeObject(obj);
			oObject.close();
		} catch (IOException e) {
			// Ignored exception
		}		
	}
	
    /**
     * Returns a copy of the object, or null if the object cannot
     * be serialized.
     */
    public static Object copyObject(Object orig) {
        Object obj = null;
        try {
            // Write the object out to a byte array
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(orig);
            out.flush();
            out.close();

            // Make an input stream from the byte array and read
            // a copy of the object back in.
            ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(bos.toByteArray()));
            obj = in.readObject();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        catch(ClassNotFoundException cnfe) {
            cnfe.printStackTrace();
        }
        return obj;
    }		
	
	private static void removeFile(String path, String filename) {
		File f = new File(path + "/" + filename);
		if (f.exists() && f.isFile()) f.delete();
	}	
	
	
	/** Constructor. The oldGraph is set to null. */
	@SuppressWarnings("unchecked")
	public Graph(String name, String path) {
		this.name       = name;
		this.path       = path;
		
		tweetSet = (Hashtable<Long, Long>)loadObject(path, name + "__TweetSet");
		if ( tweetSet == null ) tweetSet = new Hashtable<Long,Long>();
		
		tweetList = new ArrayList<Long>();
		tweetList.addAll(tweetSet.keySet());
		
		mentioned  = (Hashtable<Long, HashSet<Long>>)loadObject(path, name + "__Mention");
		if (mentioned == null) mentioned = new Hashtable<Long, HashSet<Long>>();
		
		follows  = (Hashtable<Long, HashSet<Long>>)loadObject(path, name + "__Follows");
		if (follows == null) follows = new Hashtable<Long, HashSet<Long>>();
		
		refTweets  = (Hashtable<Long, Long>)loadObject(path, name + "__RefTweets");
		if (refTweets == null) refTweets = new Hashtable<Long, Long>();
		
		userTweets  = (Hashtable<Long, HashSet<Long>>)loadObject(path, name + "__UserTweets");
		if (userTweets == null) userTweets = new Hashtable<Long, HashSet<Long>>();		
		
		hashtagsByTweet  = (Hashtable<Long, HashSet<String>>)loadObject(path, name + "__HashtagsByTweet");
		if (hashtagsByTweet == null) hashtagsByTweet = new Hashtable<Long, HashSet<String>>();	
		
		tweetsByHashtag  = (Hashtable<String, HashSet<Long>>)loadObject(path, name + "__TweetsByHashtag");
		if (tweetsByHashtag == null) tweetsByHashtag = new Hashtable<String, HashSet<Long>>();		
	}
	
	/** Copy constructor. */
	@SuppressWarnings("unchecked")
	public Graph(Graph g, String name, String path) {
		this.name = name;
		this.path = path;
		
		tweetSet = (Hashtable<Long, Long>) copyObject(g.tweetSet);
		tweetList = (ArrayList<Long>) copyObject(g.tweetList);
		mentioned = (Hashtable<Long, HashSet<Long>>) copyObject(g.mentioned);
		follows = (Hashtable<Long, HashSet<Long>>) copyObject(g.follows);
		refTweets = (Hashtable<Long, Long>) copyObject(g.refTweets);
		userTweets = (Hashtable<Long, HashSet<Long>>) copyObject(g.userTweets);
		hashtagsByTweet = (Hashtable<Long, HashSet<String>>) copyObject(g.hashtagsByTweet);
		tweetsByHashtag = (Hashtable<String, HashSet<Long>>) copyObject(g.tweetsByHashtag);
	}
	
	public void store() {
		saveObject(path, name + "__TweetSet", tweetSet);
		saveObject(path, name + "__Mention", mentioned);
		saveObject(path, name + "__Follows", follows);
		saveObject(path, name + "__RefTweets", refTweets);
		saveObject(path, name + "__UserTweets", userTweets);
		saveObject(path, name + "__HashtagsByTweet", hashtagsByTweet);
		saveObject(path, name + "__TweetsByHashtag", tweetsByHashtag);
	}

	/** Delete the files of the current graph. The graph MUST NOT BE USED AGAIN. */
	public void deletePersistentStorage() {
		removeFile(path, name + "__TweetSet");
		removeFile(path, name + "__Mention");
		removeFile(path, name + "__Follows");
		removeFile(path, name + "__RefTweets");
		removeFile(path, name + "__UserTweets");
		removeFile(path, name + "__HashtagsByTweet");
		removeFile(path, name + "__TweetsByHashtag");
	}

	private void addTweet(Long tweetID, Long userID) {
		if ( !tweetSet.contains(tweetID) )
			tweetList.add(tweetID);

		// In case we add the userID later, we need to override the previous value in tweetSet
		if (tweetSet.get(tweetID) == null) 
			tweetSet.put(tweetID, userID);
	}

	private void addAllTweets(List<Long> tweetIDs, Long userID) {
		for (Long tid : tweetIDs)
			addTweet(tid, userID);
	}

	public void addRefTweets(Long tweetID, Long refTweetID) {
		addTweet(tweetID, null);
		addTweet(refTweetID, null);
		refTweets.put(tweetID, refTweetID);
	}

	public void addUserTweets(Long userID, List<Long> tweetIDs) {
		HashSet<Long> curr_list = userTweets.get(userID);
		if (curr_list == null) curr_list = new HashSet<Long>();
		curr_list.addAll(tweetIDs);
		addAllTweets(tweetIDs, userID);
		userTweets.put(userID, curr_list);
	}

	public void addMentioned(Long tweetID, List<Long> userIDs) {
		HashSet<Long> curr_list = mentioned.get(tweetID);
		if (curr_list == null) curr_list = new HashSet<Long>();
		curr_list.addAll(userIDs);
		addTweet(tweetID, null);
		mentioned.put(tweetID, curr_list);
	}

	public void addFollows(Long userID, List<Long> userIDs) {
		HashSet<Long> curr_list = follows.get(userID);
		if (curr_list == null) curr_list = new HashSet<Long>();
		curr_list.addAll(userIDs);
		follows.put(userID, curr_list);
	}

	public void addHashtags(Long tweetID, List<String> hashtags) {
		HashSet<String> curr_list = hashtagsByTweet.get(tweetID);
		if (curr_list == null) curr_list = new HashSet<String>();
		curr_list.addAll(hashtags);
		hashtagsByTweet.put(tweetID, curr_list);

		// Transpose the list
		for(String ht : hashtags) {
			HashSet<Long> tweets = tweetsByHashtag.get(ht);
			if (tweets == null) tweets = new HashSet<Long>();
			tweets.add(tweetID);
			tweetsByHashtag.put(ht, tweets);
		}

		addTweet(tweetID, null);
	}

	public Long getRandomTweet(Random r) {
		return tweetList.get(r.nextInt(tweetList.size()));
	}
	
	public Long getRefTweet(Long tweetID) {
		if ( tweetID == null ) return null;
		return refTweets.get(tweetID);
	}	
	
	public String[] getHashtagsByTweet(Long tweetID) {
		if ( tweetID == null ) return null;
		Set<String> set = hashtagsByTweet.get(tweetID);
		if (set == null) return null;
		else return set.toArray(new String[0]);
	}

	public Long[] getTweetsByHashtag(String hashtag) {
		if ( hashtag == null ) return null;
		Set<Long> set = tweetsByHashtag.get(hashtag);
		if (set == null) return null;
		else return set.toArray(new Long[0]);		
	}	

	public Long[] getUserTweets(Long userID) {
		if ( userID == null ) return null;
		Set<Long> set = userTweets.get(userID);
		if (set == null) return null;
		else return set.toArray(new Long[0]);
	}

	public Long[] getMentionedUsers(Long tweetID) {
		if ( tweetID == null ) return null;
		Set<Long> set = mentioned.get(tweetID);
		if (set == null) return null;
		else return set.toArray(new Long[0]);
	}

	public Long[] getFollowingUsers(Long userID) {
		if ( userID == null ) return null;
		Set<Long> set = follows.get(userID);
		if (set == null) return null;
		else return set.toArray(new Long[0]);		
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
		for( HashSet<Long> l : follows.values() )
			Tfollowed += l.size();
		return Tfollowed/follows.size();
	}
	
	public double getAverageMentionsPerTweet() {
		if( tweetSet.size() == 0 ) return 0.0;

		int Tmentions = 0;
		for( HashSet<Long> l : mentioned.values() )
			Tmentions += l.size();
		return Tmentions/(double)tweetSet.size();
	}
	
	public String getPath() {
		return path;
	}
	
	public String getName() {
		return name;
	}
}