package graph;


import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;

public class PersistentGraph {
	private static final Logger logger = Logger.getLogger("ranker.logger");
	
	private final ReentrantLock tweetsLock = new ReentrantLock(true);
	private final ReentrantLock userTweetsLock = new ReentrantLock(true);
	private final ReentrantLock mentionedLock = new ReentrantLock(true);
	private final ReentrantLock followsLock = new ReentrantLock(true);
	private final ReentrantLock refTweetsLock = new ReentrantLock(true);
	private final ReentrantLock hashtagsLock = new ReentrantLock(true);

	/** Contains all tweets */
	private Hashtable<Long,Long> tweetSet;

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
		Object robject = null;
		try {
			FileInputStream file = new FileInputStream(path + "/" + name);
			ObjectInputStream obj = new ObjectInputStream(file);
			robject = obj.readObject();
		} catch (FileNotFoundException e) {
			logger.info("File " + name + " has been created!");
		} catch (Throwable t) {
			logger.fatal("Error loading the persistent graph.", t);
		}
		return robject;
	}

	private static void saveObject(String path, String name, Object obj) {
		try {
			FileOutputStream fObject   = new FileOutputStream(path + "/" + name);
			ObjectOutputStream oObject = new ObjectOutputStream(fObject);
			oObject.writeObject(obj);
			oObject.close();
		} catch (Throwable t) {
			logger.error("Error saving the file " + path + "/" + name + "!", t);
		}		
	}

	/** Constructor. The oldGraph is set to null. */
	@SuppressWarnings("unchecked")
	public PersistentGraph(String name, String path) {
		this.name       = name;
		this.path       = path;

		tweetSet = (Hashtable<Long, Long>)loadObject(path, name + "__TweetSet");
		if ( tweetSet == null ) tweetSet = new Hashtable<Long,Long>();

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

	public void store() {
		lockAll();
		try {
			saveObject(path, name + "__TweetSet", tweetSet);
			saveObject(path, name + "__Mention", mentioned);
			saveObject(path, name + "__Follows", follows);
			saveObject(path, name + "__RefTweets", refTweets);
			saveObject(path, name + "__UserTweets", userTweets);
			saveObject(path, name + "__HashtagsByTweet", hashtagsByTweet);
			saveObject(path, name + "__TweetsByHashtag", tweetsByHashtag);
		} finally {
			unlockAll();
		}
	}

	public void lockAll() {
		userTweetsLock.lock();
		mentionedLock.lock();
		followsLock.lock();
		refTweetsLock.lock();
		hashtagsLock.lock();		
	}

	public void unlockAll() {
		mentionedLock.unlock();
		followsLock.unlock();
		refTweetsLock.unlock();
		hashtagsLock.unlock();
		userTweetsLock.unlock();
	}

	private void addTweet(Long tweetID, Long userID) {
		tweetsLock.lock();
		try{
			// In case we add the userID later, we need to override the previous value in tweetSet
			Long cuID = tweetSet.get(tweetID);
			if ( cuID == null || cuID.equals(-1L) ) 
				tweetSet.put(tweetID, userID);
		} catch (Throwable t) {
			logger.error("Error adding a tweet.", t);
		} finally {
			tweetsLock.unlock();
		}
	}

	private void addAllTweets(List<Long> tweetIDs, Long userID) {
		for (Long tid : tweetIDs)
			addTweet(tid, userID);
	}

	public void addRefTweets(Long tweetID, Long refTweetID) {
		refTweetsLock.lock();
		try{
			addTweet(tweetID, -1L);
			addTweet(refTweetID, -1L);
			refTweets.put(tweetID, refTweetID);
		} catch (Throwable t) {
			logger.error("Error adding a reference.", t);
		} finally {
			refTweetsLock.unlock();
		}
	}

	public void addUserTweets(Long userID, List<Long> tweetIDs) {
		userTweetsLock.lock();
		try{
			HashSet<Long> curr_list = userTweets.get(userID);
			if (curr_list == null) curr_list = new HashSet<Long>();
			curr_list.addAll(tweetIDs);
			addAllTweets(tweetIDs, userID);
			userTweets.put(userID, curr_list);
		} catch (Throwable t) {
			logger.error("Error adding a user tweet.", t);
		} finally {
			userTweetsLock.unlock();
		}
	}

	public void addMentioned(Long tweetID, List<Long> userIDs) {
		mentionedLock.lock();
		try{
			HashSet<Long> curr_list = mentioned.get(tweetID);
			if (curr_list == null) curr_list = new HashSet<Long>();
			curr_list.addAll(userIDs);
			addTweet(tweetID, -1L);
			mentioned.put(tweetID, curr_list);
		} catch (Throwable t) {
			logger.error("Error adding a mention.", t);
		} finally {
			mentionedLock.unlock();
		}
	}

	public void addFollows(Long userID, List<Long> userIDs) {
		followsLock.lock();
		try{
			HashSet<Long> curr_list = follows.get(userID);
			if (curr_list == null) curr_list = new HashSet<Long>();
			curr_list.addAll(userIDs);
			follows.put(userID, curr_list);
		} catch (Throwable t) {
			logger.error("Error adding a friend.", t);
		} finally {
			followsLock.unlock();
		}
	}

	public void addHashtags(Long tweetID, List<String> hashtags) {
		hashtagsLock.lock();
		try{
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

			addTweet(tweetID, -1L);
		} catch (Throwable t) {
			logger.error("Error adding a hashtag.", t);
		} finally {
			hashtagsLock.unlock();
		}
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

	/**
	 * @return the tweetSet
	 */
	public Hashtable<Long, Long> getTweetSet() {
		return tweetSet;
	}

	/**
	 * @return the userTweets
	 */
	public Hashtable<Long, HashSet<Long>> getUserTweets() {
		return userTweets;
	}

	/**
	 * @return the mentioned
	 */
	public Hashtable<Long, HashSet<Long>> getMentioned() {
		return mentioned;
	}

	/**
	 * @return the follows
	 */
	public Hashtable<Long, HashSet<Long>> getFollows() {
		return follows;
	}

	/**
	 * @return the refTweets
	 */
	public Hashtable<Long, Long> getRefTweets() {
		return refTweets;
	}

	/**
	 * @return the hashtagsByTweet
	 */
	public Hashtable<Long, HashSet<String>> getHashtagsByTweet() {
		return hashtagsByTweet;
	}

	/**
	 * @return the tweetsByHashtag
	 */
	public Hashtable<String, HashSet<Long>> getTweetsByHashtag() {
		return tweetsByHashtag;
	}
}