package graph;


import java.io.*;
import java.util.*;
import org.apache.log4j.Logger;

public class PersistentGraph {
	private static final Logger logger = Logger.getLogger("ranker.logger");

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
			logger.fatal("Error loading the persistent graph (File " + name + ").", t);
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

		tweetSet = (HashMap<Long, Long>)loadObject(path, name + "__TweetSet");
		if ( tweetSet == null ) tweetSet = new HashMap<Long,Long>();

		mentioned  = (HashMap<Long, HashSet<Long>>)loadObject(path, name + "__Mention");
		if (mentioned == null) mentioned = new HashMap<Long, HashSet<Long>>();

		follows  = (HashMap<Long, HashSet<Long>>)loadObject(path, name + "__Follows");
		if (follows == null) follows = new HashMap<Long, HashSet<Long>>();

		refTweets  = (HashMap<Long, Long>)loadObject(path, name + "__RefTweets");
		if (refTweets == null) refTweets = new HashMap<Long, Long>();

		userTweets  = (HashMap<Long, HashSet<Long>>)loadObject(path, name + "__UserTweets");
		if (userTweets == null) userTweets = new HashMap<Long, HashSet<Long>>();		

		hashtagsByTweet  = (HashMap<Long, HashSet<String>>)loadObject(path, name + "__HashtagsByTweet");
		if (hashtagsByTweet == null) hashtagsByTweet = new HashMap<Long, HashSet<String>>();	

		tweetsByHashtag  = (HashMap<String, HashSet<Long>>)loadObject(path, name + "__TweetsByHashtag");
		if (tweetsByHashtag == null) tweetsByHashtag = new HashMap<String, HashSet<Long>>();		
	}

	public void store() {
		synchronized ( tweetSet ) {
			synchronized ( userTweets ) {
				synchronized ( mentioned ) {
					synchronized ( follows ) {
						synchronized ( refTweets ) {
							synchronized (hashtagsByTweet) {
								synchronized (tweetsByHashtag) {
									saveObject(path, name + "__TweetSet", tweetSet);
									saveObject(path, name + "__Mention", mentioned);
									saveObject(path, name + "__Follows", follows);
									saveObject(path, name + "__RefTweets", refTweets);
									saveObject(path, name + "__UserTweets", userTweets);
									saveObject(path, name + "__HashtagsByTweet", hashtagsByTweet);
									saveObject(path, name + "__TweetsByHashtag", tweetsByHashtag);
								}
							}
						}
					}
				}
			}
		}
	}

	private void addTweet(Long tweetID, Long userID) {
		synchronized ( tweetSet ) {
			try{
				// In case we add the userID later, we need to override the previous value in tweetSet
				Long cuID = tweetSet.get(tweetID);
				if ( cuID == null || cuID.equals(-1L) ) 
					tweetSet.put(tweetID, userID);
			} catch (Throwable t) {
				logger.error("Error adding a tweet.", t);
			}
		}
	}

	private void addAllTweets(List<Long> tweetIDs, Long userID) {
		for (Long tid : tweetIDs)
			addTweet(tid, userID);
	}

	public void addRefTweets(Long tweetID, Long refTweetID) {
		synchronized (refTweets) {
			try{
				addTweet(tweetID, -1L);
				addTweet(refTweetID, -1L);
				refTweets.put(tweetID, refTweetID);
			} catch (Throwable t) {
				logger.error("Error adding a reference.", t);
			}
		}
	}

	public void addUserTweets(Long userID, List<Long> tweetIDs) {
		synchronized (userTweets) {
			try{
				HashSet<Long> curr_list = userTweets.get(userID);
				if (curr_list == null) curr_list = new HashSet<Long>();
				curr_list.addAll(tweetIDs);
				addAllTweets(tweetIDs, userID);
				userTweets.put(userID, curr_list);
			} catch (Throwable t) {
				logger.error("Error adding a user tweet.", t);
			}
		}
	}

	public void addMentioned(Long tweetID, List<Long> userIDs) {
		synchronized (mentioned) {
			try{
				HashSet<Long> curr_list = mentioned.get(tweetID);
				if (curr_list == null) curr_list = new HashSet<Long>();
				curr_list.addAll(userIDs);
				addTweet(tweetID, -1L);
				mentioned.put(tweetID, curr_list);
			} catch (Throwable t) {
				logger.error("Error adding a mention.", t);
			}
		}
	}

	public void addFollows(Long userID, List<Long> userIDs) {
		synchronized ( follows ) {
			try{
				HashSet<Long> curr_list = follows.get(userID);
				if (curr_list == null) curr_list = new HashSet<Long>();
				curr_list.addAll(userIDs);
				follows.put(userID, curr_list);
			} catch (Throwable t) {
				logger.error("Error adding a friend.", t);
			}
		}
	}

	public void addHashtags(Long tweetID, List<String> hashtags) {
		synchronized ( hashtagsByTweet ) {
			try{
				HashSet<String> curr_list = hashtagsByTweet.get(tweetID);
				if (curr_list == null) curr_list = new HashSet<String>();
				curr_list.addAll(hashtags);
				hashtagsByTweet.put(tweetID, curr_list);

				synchronized ( tweetsByHashtag ) {
					// Transpose the list
					for(String ht : hashtags) {
						HashSet<Long> tweets = tweetsByHashtag.get(ht);
						if (tweets == null) tweets = new HashSet<Long>();
						tweets.add(tweetID);
						tweetsByHashtag.put(ht, tweets);
					}
				}

				addTweet(tweetID, -1L);
			} catch (Throwable t) {
				logger.error("Error adding a hashtag.", t);
			}
		}
	}

	public int getNumberOfTweets() {
		int size = 0;
		synchronized ( tweetSet ) { size = tweetSet.size(); }
		return size;
	}

	public int getNumberOfReferences() {
		int size = 0;
		synchronized ( refTweets ) { size = refTweets.size(); }
		return size;
	}

	public int getNumberOfUsers() {
		int size = 0;
		synchronized ( userTweets ) { size = userTweets.size(); }
		return size;
	}

	public int getNumberOfHashtags() {
		int size = 0;
		synchronized ( tweetsByHashtag ) { size = tweetsByHashtag.size(); }
		return size;
	}

	public double getAverageTweetsPerUser() {
		int usSize = getNumberOfUsers();
		int twSize = getNumberOfTweets();
		return twSize/(double)usSize;
	}

	public double getAverageReferencePerTweet() {
		int twSize = getNumberOfTweets();
		int rfSize = getNumberOfReferences();
		return rfSize/(double)twSize;
	}	

	/**
	 * The Effective Friends are those friends who have posted
	 * some tweet. This method returns the mean of the
	 * effective friends that users have.
	 * @return The average number of effective friends per user. 
	 */
	public double getAverageEffectiveFriendsPerUser() {
		double avg_fpu = 0.0;
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

		return avg_fpu;
	}

	public double getAverageMentionsPerTweet() {
		int twSize = getNumberOfTweets();
		double avg_mpt = 0.0;
		if ( twSize > 0 ) {
			int Tmentions = 0;
			synchronized ( mentioned ) {
				for( HashSet<Long> l : mentioned.values() )
					Tmentions += l.size();				
			}
			avg_mpt = Tmentions/twSize;
		}
		return avg_mpt;
	}

	public double getAverageHashtagsPerTweet() {
		int twSize = getNumberOfTweets();
		double avg_hpt = 0.0;
		if ( twSize > 0 ) {
			int Thashtags = 0;
			synchronized(hashtagsByTweet) {
				for( HashSet<String> l : hashtagsByTweet.values() )
					Thashtags += l.size();
			}
			avg_hpt = Thashtags/(double)tweetSet.size();
		}
		return avg_hpt;
	}

	private static <K,V> HashMap<K,ArrayList<V>> convertHashMap(Map<K, HashSet<V>> in) {
		HashMap<K,ArrayList<V>> out = new HashMap<K,ArrayList<V>>();
		for ( Map.Entry<K, HashSet<V>> entry : in.entrySet() ) {
			out.put(entry.getKey(), new ArrayList<V>(entry.getValue()));
		}
		return out;
	}

	private static HashMap<Long,ArrayList<Long>> convertFilteredFriends(Map<Long, HashSet<Long>> friends, 
			HashMap<Long,HashSet<Long>> userTweets) {
		HashMap<Long, ArrayList<Long>> filtered_friends = new HashMap<Long, ArrayList<Long>>(); 
		for ( Map.Entry<Long, HashSet<Long>> entry : friends.entrySet() ) {
			Long user = entry.getKey(); // Current user
			ArrayList<Long> f_userfriends = new ArrayList<Long>(); // Filtered list of user's friends

			// Traverses all the user's friends
			for( Long friend : entry.getValue() ) { 
				// If the friend has posted some tweet, then add the friend to the filtered list of friends
				HashSet<Long> tweetsByFriend = userTweets.get(friend);
				if ( tweetsByFriend != null && tweetsByFriend.size() > 0 )
					f_userfriends.add(friend);
			}

			filtered_friends.put(user, f_userfriends);
		}

		return filtered_friends;
	}	

	public TemporaryGraph createTemporaryGraph () {
		HashMap<Long,Long> tTweetSet = null;
		HashMap<Long,Long> tRefTweets = null;
		HashMap<Long,ArrayList<Long>> tUserTweets = null;
		HashMap<Long,ArrayList<Long>> tMentioned = null;
		HashMap<Long,ArrayList<Long>> tFollows = null;
		HashMap<Long,ArrayList<String>> tHashtagsByTweet = null;
		HashMap<String,ArrayList<Long>> tTweetsByHashtag = null;
		synchronized ( tweetSet ) {
			synchronized ( userTweets ) {
				synchronized ( mentioned ) {
					synchronized ( follows ) {
						synchronized ( refTweets ) {
							synchronized (hashtagsByTweet) {
								synchronized (tweetsByHashtag) {
									try {
					 					tTweetSet = new HashMap<Long,Long>(tweetSet);
										tRefTweets = new HashMap<Long,Long>(refTweets);
										tUserTweets = convertHashMap(userTweets);
										tMentioned = convertHashMap(mentioned);
										tFollows = convertFilteredFriends(follows, userTweets);
										tHashtagsByTweet = convertHashMap(hashtagsByTweet);
										tTweetsByHashtag = convertHashMap(tweetsByHashtag);
									} catch ( Exception e ) {
										logger.error("Error creating temporal graph.", e);
									}
								}
							}

						}

					}
				}
			}
		}

		if ( tTweetSet != null && tRefTweets != null && tUserTweets != null && tMentioned != null && 
				tFollows != null && tHashtagsByTweet != null && tTweetsByHashtag != null )
			return new TemporaryGraph(tTweetSet, tRefTweets, tUserTweets, tMentioned, tFollows, tHashtagsByTweet, tTweetsByHashtag);
		else
			return null;
	}
}