package httpserv;

import graph.PersistentGraph;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;

import org.apache.log4j.Logger;

import utils.Time;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import computer.TweetRankComputer;

/** This class handles the STATUS requests. */
public class StatusHandler implements HttpHandler {
	private static final Logger logger = Logger.getLogger("ranker.logger");
	private PersistentGraph pgraph;
	private TweetRankComputer trcomputer;

	public StatusHandler(PersistentGraph pgraph, TweetRankComputer trcomputer) {
		super();
		this.pgraph = pgraph;
		this.trcomputer = trcomputer;
	}

	@Override
	public void handle(HttpExchange t) throws IOException {
		try {
			String response = "Persistent graph info:\n======================\n" +
			"Number of tweets: " + pgraph.getNumberOfTweets() + "\n" + 
			"Number of users: " + pgraph.getNumberOfUsers() + "\n" +
			"Number of hashtags: " + pgraph.getNumberOfHashtags() + "\n" +
			"Average tweets per user: " + pgraph.getAverageTweetsPerUser() + "\n" +
			"Average effective friends per user: " + pgraph.getAverageEffectiveFriendsPerUser() + "\n" +
			"Average references per tweet: " + pgraph.getAverageReferencePerTweet() + "\n" +
			"Average mentions per tweet: " + pgraph.getAverageMentionsPerTweet() + "\n" +
			"Average hashtags per tweet: " + pgraph.getAverageHashtagsPerTweet() + "\n\n";

			response += "TweetRank computation:\n======================\n";
			TweetRankComputer.State state = trcomputer.getState();
			Date enddate = trcomputer.getEndDate();
			Time elapsed = trcomputer.getElapsedTime();

			if ( state == TweetRankComputer.State.WORKING )	response += "State: WORKING\n"; 
			else response += "State: IDLE\n";

			if ( trcomputer.getTemporaryGraph() != null ) {
				response += "Number of tweets: " + trcomputer.getTemporaryGraph().getNumberOfTweets() + "\n" + 
				"Number of users: " + trcomputer.getTemporaryGraph().getNumberOfUsers() + "\n" +
				"Number of hashtags: " + trcomputer.getTemporaryGraph().getNumberOfHashtags() + "\n" +
				"Average tweets per user: " + trcomputer.getTemporaryGraph().getAverageTweetsPerUser() + "\n" +
				"Average effective friends per user: " + trcomputer.getTemporaryGraph().getAverageEffectiveFriendsPerUser() + "\n" +
				"Average references per tweet: " + trcomputer.getTemporaryGraph().getAverageReferencePerTweet() + "\n" +
				"Average mentions per tweet: " + trcomputer.getTemporaryGraph().getAverageMentionsPerTweet() + "\n" +
				"Average hashtags per tweet: " + trcomputer.getTemporaryGraph().getAverageHashtagsPerTweet() + "\n\n";
			} else {
				response += "Temporary graph not initialized.\n\n";
			}
			response += "Last computation: " + (enddate == null ? "Never" : Time.formatDate("yyyy/MM/dd HH:mm:ss", enddate)) + "\n";
			if ( elapsed != null )	response += "Elapsed time: " + elapsed;


			t.sendResponseHeaders(200, response.length());
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
		} catch ( Throwable th ) {
			logger.error("Error during status recopilation.", th);
			String response = "Error during status recopilation.";
			t.sendResponseHeaders(400, response.length());
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
		}
	}
}
