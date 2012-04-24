
import java.util.*;

public class TweetRanker {

	/*probabilities (15% for each to happen right now, should probably be changed!)*/
	private final double RANDOM_JUMP = 0.15;
	private final double VISIT_MENTIONED_USER = 0.30;
	private final double VISIT_FOLLOWED_USER = 0.45;
	
	/** Mapping between a tweet and how many times it's been visited by the surfer */
	private Hashtable<String, Integer> tweetVisited = new Hashtable<String, Integer>();
	
	/** Maps users to tweets */
	private Hashtable<String, List<String>> userTweets = new Hashtable<String, List<String>>();
	
	/** Contains all tweets */
	private HashSet<String> tweet_set = new HashSet<String>();
        private ArrayList<String> tweets = new ArrayList<String>();
		
	/** Maps a tweet to a list of user mentions */
	private Hashtable<String, List<String>> mentioned = new Hashtable<String, List<String>>();
	
	/** Maps a user to a list of users he/she follows */
	private Hashtable<String, List<String>> follows = new Hashtable<String, List<String>>();
	
	/** Map a reply/retweet to the original tweet */
	private Hashtable<String, String> refTweets = new Hashtable<String, String>();

        /** Map a tweet to a list of hashtags */
        private Hashtable<String, List<String>> tweetHashtags = new Hashtable<String, List<String>>();
	
	public void computePageRank() {
	    int n = tweets.size();
	    int m = n/10;
	    MCCompletePath(n, m);
	}

        private void addTweet(String tweetID) {
	    if ( !tweet_set.contains(tweetID) ) {
	        tweet_set.add(tweetID);
		tweets.add(tweetID);
	    }
	}

        private void addAllTweets(List<String> tweetIDs) {
	    for (String tid : tweetIDs)
		addTweet(tid);
        } 
	
	public void addRefTweets(String tweetID, String refTweetID) {
	    refTweets.put(tweetID, refTweetID);
	    addTweet(tweetID);
	    addTweet(refTweetID);
	}
	
	public void addUserTweets(String userID, List<String> tweetIDs) {
	    List<String> curr_list = userTweets.get(userID);
	    if (curr_list == null) userTweets.put(userID, tweetIDs);
	    else curr_list.addAll(tweetIDs);
	    addAllTweets(tweetIDs);
	}
	
	public void addMentioned(String tweetID, List<String> userIDs) {
	    List<String> curr_list = mentioned.get(tweetID);
	    if (curr_list == null) mentioned.put(tweetID, userIDs);
	    else curr_list.addAll(userIDs);
	    addTweet(tweetID);
	}
	
	public void addFollows(String userID, List<String> userIDs) {
	    List<String> curr_list = follows.get(userID);
	    if (curr_list == null) follows.put(userID, userIDs);
	    else curr_list.addAll(userIDs);
	}

        public void addHashtags(String tweetID, List<String> hashtags) {
	    List<String> curr_list = tweetHashtags.get(tweetID);
	    if (curr_list == null) tweetHashtags.put(tweetID, hashtags);
	    else curr_list.addAll(hashtags);
	    addTweet(tweetID);
        }
	
	private void addVisit(String tweetID) {
		if(tweetVisited.containsKey(tweetID))
			tweetVisited.put(tweetID, tweetVisited.get(tweetID)+1);
		else
			tweetVisited.put(tweetID, 1);
	}
	
	private String randomJump(Random r) {
		return tweets.get(r.nextInt(tweets.size()));
	}
	
	private String jumpUserTweet(String tweetID, Random r, Hashtable<String, List<String>> ht) {
		List<String> mentionedUsers = ht.get(tweetID);
		String randomUser = mentionedUsers.get(r.nextInt(mentionedUsers.size()));
		List<String> tweetsOfUser = userTweets.get(randomUser);
		return tweetsOfUser.get(r.nextInt(mentionedUsers.size()));
	}
	
	private String jumpReferenceTweet(String tweetID) {
		return refTweets.get(tweetID);
	}
	
	private void MCCompletePath(int numberOfDocs, int m) {
		Random r = new Random();
		double random;
		
		for(int i = 1; i <= m; ++i) {
			for(String currentID : tweets) {
				random = r.nextDouble();
				if(random <= RANDOM_JUMP) {
					currentID = randomJump(r);
					addVisit(currentID);
					continue;
				}
				random+=RANDOM_JUMP;
				if(mentioned.containsKey(currentID) && random <= VISIT_MENTIONED_USER) {
					currentID = jumpUserTweet(currentID, r, mentioned);
					addVisit(currentID);
					continue;
				}
				random+=VISIT_MENTIONED_USER;
				if(follows.containsKey(currentID) && random <= VISIT_FOLLOWED_USER) {
					currentID = jumpUserTweet(currentID, r, follows);
					addVisit(currentID);
					continue;
				}
				if(refTweets.containsKey(currentID)) {
					currentID = jumpReferenceTweet(currentID);
					addVisit(currentID);
					continue;
				}
				currentID = randomJump(r);
				addVisit(currentID);
			}
		}
		
		Iterator<String> itr = tweetVisited.keySet().iterator();
		String tweetID;
		int tweetRank;
		while(itr.hasNext()) {
			tweetID = itr.next();
			tweetRank = tweetVisited.get(tweetID);
			tweetRank = (tweetRank+m)/tweets.size();
			System.out.println("tweetID : "+tweetRank);
		}
	}
}
