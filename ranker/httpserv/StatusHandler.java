package httpserv;

import graph.PersistentGraph;

import java.io.IOException;
import java.io.OutputStream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/** This class handles the STATUS requests. */
public class StatusHandler implements HttpHandler {
	private PersistentGraph graph;

	public StatusHandler(PersistentGraph graph) {
		super();
		this.graph = graph;
	}

	@Override
	public void handle(HttpExchange t) throws IOException {
		String response = "";

		response = "Number of tweets: " + graph.getNumberOfTweets() + "\n" + 
		"Number of users: " + graph.getNumberOfUsers() + "\n" +
		"Number of hashtags: " + graph.getNumberOfHashtags() + "\n" +
		"Average tweets per user: " + graph.getAverageTweetsPerUser() + "\n" +
		"Average friends per user: " + graph.getAverageFriendsPerUser() + "\n" +
		"Average references per tweet: " + graph.getAverageReferencePerTweet() + "\n" +
		"Average mentions per tweet: " + graph.getAverageMentionsPerTweet();

		t.sendResponseHeaders(200, response.length());
		OutputStream os = t.getResponseBody();
		os.write(response.getBytes());
		os.close();
	}
}
