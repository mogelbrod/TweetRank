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
	private HashMap<Long,Long> tweetSet;

	/** Maps users to tweets */
	private HashMap<Long,HashSet<Long>> userTweets;

	/** Maps a tweet to a list of user mentions */
	private HashMap<Long,HashSet<Long>> mentioned;

	/** Maps a user to a list of users he/she follows */
	private HashMap<Long,HashSet<Long>> follows;

	/** Map a reply/retweet to the original tweet */
	private HashMap<Long,Long> refTweets;

	/** Map a tweet to a list of hashtags */
	private HashMap<Long,HashSet<String>> hashtagsByTweet;
	private HashMap<String,HashSet<Long>> tweetsByHashtag;

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
	public PersistentGraph(String name, String path, Integer version) {
		tweetSet = (HashMap<Long, Long>)loadObject(path, name + "__TweetSet-" + version);
		if ( tweetSet == null ) tweetSet = new HashMap<Long,Long>();

		mentioned  = (HashMap<Long, HashSet<Long>>)loadObject(path, name + "__Mention-" + version);
		if (mentioned == null) mentioned = new HashMap<Long, HashSet<Long>>();

		follows  = (HashMap<Long, HashSet<Long>>)loadObject(path, name + "__Follows-" + version);
		if (follows == null) follows = new HashMap<Long, HashSet<Long>>();

		refTweets  = (HashMap<Long, Long>)loadObject(path, name + "__RefTweets-" + version);
		if (refTweets == null) refTweets = new HashMap<Long, Long>();

		userTweets  = (HashMap<Long, HashSet<Long>>)loadObject(path, name + "__UserTweets-" + version);
		if (userTweets == null) userTweets = new HashMap<Long, HashSet<Long>>();		

		hashtagsByTweet  = (HashMap<Long, HashSet<String>>)loadObject(path, name + "__HashtagsByTweet-" + version);
		if (hashtagsByTweet == null) hashtagsByTweet = new HashMap<Long, HashSet<String>>();	

		tweetsByHashtag  = (HashMap<String, HashSet<Long>>)loadObject(path, name + "__TweetsByHashtag-" + version);
		if (tweetsByHashtag == null) tweetsByHashtag = new HashMap<String, HashSet<Long>>();		
	}

	public void store(String path, String name, Integer version) {
		lockAll();
		try {
			saveObject(path, name + "__TweetSet-" + version, tweetSet);
			saveObject(path, name + "__Mention-" + version, mentioned);
			saveObject(path, name + "__Follows-" + version, follows);
			saveObject(path, name + "__RefTweets-" + version, refTweets);
			saveObject(path, name + "__UserTweets-" + version, userTweets);
			saveObject(path, name + "__HashtagsByTweet-" + version, hashtagsByTweet);
			saveObject(path, name + "__TweetsByHashtag-" + version, tweetsByHashtag);
		} finally {
			unlockAll();
		}
	}

	private void lockAll() {
		userTweetsLock.lock();
		mentionedLock.lock();
		followsLock.lock();
		refTweetsLock.lock();
		hashtagsLock.lock();		
	}

	private void unlockAll() {
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

	/**
	 * The Effective Friends are those friends who have posted
	 * some tweet. This method returns the mean of the
	 * effective friends that users have.
	 * @return The average number of effective friends per user. 
	 */
	public double getAverageEffectiveFriendsPerUser() {
		double avg_fpu = 0.0;
		followsLock.lock();
		userTweetsLock.lock();
		try {
			if (follows.size() > 0)  {
				int Tfriends = 0;
				for( HashSet<Long> friendsSet : follows.values() ) {
					for ( Long friend : friendsSet ) {
						HashSet<Long> tweets_by_friend = userTweets.get(friend);
						if (tweets_by_friend != null && tweets_by_friend.size() > 0) 
							Tfriends++;
					}
				}
				avg_fpu = Tfriends/(double)follows.size();
			}
		} finally {
			userTweetsLock.unlock();
			followsLock.unlock();
		}
		return avg_fpu;
	}

	public double getAverageMentionsPerTweet() {
		double avg_mpt = 0.0;
		mentionedLock.lock();
		tweetsLock.lock();
		try {
			if ( tweetSet.size() > 0 ) {
				int Tmentions = 0;
				for( HashSet<Long> l : mentioned.values() )
					Tmentions += l.size();
				avg_mpt = Tmentions/(double)tweetSet.size();
			}
		} finally {
			tweetsLock.unlock();
			mentionedLock.unlock();
		}
		return avg_mpt;
	}

	public double getAverageHashtagsPerTweet() {
		double avg_hpt = 0.0;
		hashtagsLock.lock();
		tweetsLock.lock();
		try {
			if ( tweetSet.size() > 0 ) {
				int Thashtags = 0;
				for( HashSet<String> l : hashtagsByTweet.values() )
					Thashtags += l.size();
				avg_hpt = Thashtags/(double)tweetSet.size();
			}
		} finally {
			tweetsLock.unlock();
			hashtagsLock.unlock();
		}
		return avg_hpt;
	}
}