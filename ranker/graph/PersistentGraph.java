package graph;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

public class PersistentGraph extends Graph <HashSet<Long>, HashSet<String>>{
	private static final Logger logger = Logger.getLogger("ranker.logger");
	private final ReentrantLock tweetsLock = new ReentrantLock(true);
	private final ReentrantLock userTweetsLock = new ReentrantLock(true);
	private final ReentrantLock mentionedLock = new ReentrantLock(true);
	private final ReentrantLock followsLock = new ReentrantLock(true);
	private final ReentrantLock refTweetsLock = new ReentrantLock(true);
	private final ReentrantLock hashtagsLock = new ReentrantLock(true);

	private boolean modified_graph;


	@SuppressWarnings("unchecked")
	public PersistentGraph(String path, String prefix, Integer version) {
		lockAll();
		try {
			tweets = (HashMap<Long, Long>)utils.Functions.loadObject(path, prefix + "__TweetSet-" + version);
			if ( tweets == null ) {
				logger.info("New file " + prefix + "__TweetSet-" + version + " used");
				tweets = new HashMap<Long,Long>();
			}

			mentions  = (HashMap<Long, HashSet<Long>>)utils.Functions.loadObject(path, prefix + "__Mention-" + version);
			if (mentions == null) {
				logger.info("New file " + prefix + "__Mention-" + version + " used");
				mentions = new HashMap<Long, HashSet<Long>>();
			}

			friends  = (HashMap<Long, HashSet<Long>>)utils.Functions.loadObject(path, prefix + "__Follows-" + version);
			if (friends == null) {
				logger.info("New file " + prefix + "__Follows-" + version + " used");
				friends = new HashMap<Long, HashSet<Long>>();
			}

			references  = (HashMap<Long, Long>)utils.Functions.loadObject(path, prefix + "__RefTweets-" + version);
			if (references == null) {
				logger.info("New file " + prefix + "__RefTweets-" + version + " used");
				references = new HashMap<Long, Long>();
			}

			tweetsByUser  = (HashMap<Long, HashSet<Long>>)utils.Functions.loadObject(path, prefix + "__UserTweets-" + version);
			if (tweetsByUser == null) {
				logger.info("New file " + prefix + "__UserTweets-" + version + " used");
				tweetsByUser = new HashMap<Long, HashSet<Long>>();
			}
			
			hashtagsByTweet  = (HashMap<Long, HashSet<String>>)utils.Functions.loadObject(path, prefix + "__HashtagsByTweet-" + version);
			if (hashtagsByTweet == null) {
				logger.info("New file " + prefix + "__HashtagsByTweet-" + version + " used");
				hashtagsByTweet = new HashMap<Long, HashSet<String>>();
			}

			tweetsByHashtag  = (HashMap<String, HashSet<Long>>)utils.Functions.loadObject(path, prefix + "__TweetsByHashtag-" + version);
			if (tweetsByHashtag == null) {
				logger.info("New file " + prefix + "__TweetsByHashtag-" + version + " used");
				tweetsByHashtag = new HashMap<String, HashSet<Long>>();
			}

			modified_graph = false;
		} catch (Throwable t) {
			logger.fatal(t.getMessage(), t);
		} finally {
			unlockAll();
		}		
	}

	public void store(String path, String name, Integer version) {
		lockAll();
		try {
			if ( modified_graph ) {
				utils.Functions.saveObject(path, name + "__TweetSet-" + version, tweets);
				utils.Functions.saveObject(path, name + "__Mention-" + version, mentions);
				utils.Functions.saveObject(path, name + "__Follows-" + version, friends);
				utils.Functions.saveObject(path, name + "__RefTweets-" + version, references);
				utils.Functions.saveObject(path, name + "__UserTweets-" + version, tweetsByUser);
				utils.Functions.saveObject(path, name + "__HashtagsByTweet-" + version, hashtagsByTweet);
				utils.Functions.saveObject(path, name + "__TweetsByHashtag-" + version, tweetsByHashtag);
				modified_graph = false;
			}
		} catch (Throwable t) {
			logger.error(t.getMessage(), t);
		} finally {
			unlockAll();
		}
	}

	private void lockAll() {
		tweetsLock.lock();
		userTweetsLock.lock();
		mentionedLock.lock();
		followsLock.lock();
		refTweetsLock.lock();
		hashtagsLock.lock();		
	}

	private void unlockAll() {
		hashtagsLock.unlock();
		refTweetsLock.unlock();
		followsLock.unlock();
		mentionedLock.unlock();
		userTweetsLock.unlock();
		tweetsLock.unlock();
	}

	private void addTweet(Long tweetID, Long userID) {
		tweetsLock.lock();
		try{
			// In case we add the userID later, we need to override the previous value in tweetSet
			Long cuID = tweets.get(tweetID);
			if ( cuID == null || cuID.equals(-1L) )
				tweets.put(tweetID, userID);
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
			references.put(tweetID, refTweetID);
			modified_graph = true;
		} catch (Throwable t) {
			logger.error("Error adding a reference.", t);
		} finally {
			refTweetsLock.unlock();
		}
	}

	public void addUserTweets(Long userID, List<Long> tweetIDs) {
		userTweetsLock.lock();
		try{
			HashSet<Long> curr_list = tweetsByUser.get(userID);
			if (curr_list == null) curr_list = new HashSet<Long>();
			curr_list.addAll(tweetIDs);
			addAllTweets(tweetIDs, userID);
			tweetsByUser.put(userID, curr_list);
			modified_graph = true;
		} catch (Throwable t) {
			logger.error("Error adding a user tweet.", t);
		} finally {
			userTweetsLock.unlock();
		}
	}

	public void addMentioned(Long tweetID, List<Long> userIDs) {
		mentionedLock.lock();
		try{
			HashSet<Long> curr_list = mentions.get(tweetID);
			if (curr_list == null) curr_list = new HashSet<Long>();
			curr_list.addAll(userIDs);
			addTweet(tweetID, -1L);
			mentions.put(tweetID, curr_list);
			modified_graph = true;
		} catch (Throwable t) {
			logger.error("Error adding a mention.", t);
		} finally {
			mentionedLock.unlock();
		}
	}

	public void addFriends(Long userID, List<Long> userIDs) {
		followsLock.lock();
		try{
			HashSet<Long> curr_list = friends.get(userID);
			if (curr_list == null) curr_list = new HashSet<Long>();
			curr_list.addAll(userIDs);
			friends.put(userID, curr_list);
			modified_graph = true;
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
			modified_graph = true;
		} catch (Throwable t) {
			logger.error("Error adding a hashtag.", t);
		} finally {
			hashtagsLock.unlock();
		}
	}

	@Override
	public double getAverageFriendsPerUser() {
		double avg = 0.0;
		followsLock.lock();
		try { avg = super.getAverageFriendsPerUser(); } 
		finally { followsLock.unlock();	}
		return avg;
	}

	@Override
	public double getAverageMentionsPerTweet() {
		double avg = 0.0;
		mentionedLock.lock();
		try { avg = super.getAverageMentionsPerTweet(); }
		finally { mentionedLock.unlock(); }
		return avg;
	}

	@Override
	public double getAverageHashtagsPerTweet() {
		double avg = 0.0;
		hashtagsLock.lock();
		try { avg = super.getAverageHashtagsPerTweet(); }
		finally { hashtagsLock.unlock(); }
		return avg;
	}

	public TemporaryGraph createTemporaryGraph () {
		TemporaryGraph tempGraph = null;
		lockAll();
		try {
			HashMap<Long, Long> tTweetSet = filterActiveTweets(tweets);
			Set<Long> validTweets = tTweetSet.keySet();
			
			HashMap<Long, Long> tRefTweets = filterActiveReferences(references, tTweetSet.keySet());
			
			HashMap<Long, ArrayList<Long>> tUserTweets = utils.Functions.MapOfSetsToMapOfLists(filterActiveTweetsByUser(tweetsByUser,validTweets));
			Set<Long> validUsers = tUserTweets.keySet();
			
			HashMap<Long, ArrayList<Long>> tMentioned = utils.Functions.MapOfSetsToMapOfLists(filterActiveMentionship(mentions, validUsers));
			HashMap<Long, ArrayList<Long>> tFollows = utils.Functions.MapOfSetsToMapOfLists(filterActiveFriendship(friends, validUsers));
			HashMap<String, ArrayList<Long>> tTweetsByHashtag = utils.Functions.MapOfSetsToMapOfLists(filterActiveTweetsByHashtag(tweetsByHashtag, validTweets));
			HashMap<Long, ArrayList<String>> tHashtagsByTweet = utils.Functions.MapOfSetsToMapOfLists(filterActiveHashtagsByTweet(hashtagsByTweet, validTweets));
			
			tempGraph = new TemporaryGraph(tTweetSet, tRefTweets, tUserTweets, tMentioned, tFollows, tHashtagsByTweet, tTweetsByHashtag);
		} catch ( Exception e ) {
			logger.error("Error creating temporal graph.", e);
		} finally {
			unlockAll();
		}

		return tempGraph;
	}

	@Override
	protected HashSet<Long> filterActiveFriends(HashSet<Long> friends, Set<Long> validUsers) {
		HashSet<Long> activeFriends = new HashSet<Long>();
		for( Long friend : friends )
			if ( validUsers.contains(friend) )
				activeFriends.add(friend);
		return activeFriends;
	}

	@Override
	protected HashSet<Long> filterActiveMentions(HashSet<Long> mentions, Set<Long> validUsers) {
		HashSet<Long> activeMentions = new HashSet<Long>();
		for( Long user : mentions )
			if ( validUsers.contains(user) )
				activeMentions.add(user);
		return activeMentions;
	}

	@Override
	protected HashSet<Long> filterTweetsCollection(HashSet<Long> tweets, Set<Long> validTweets) {
		return utils.Functions.SetIntersection(tweets, validTweets);
	}
}
