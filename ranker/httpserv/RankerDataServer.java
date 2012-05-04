package httpserv;

import graph.TemporaryGraph;
import graph.PersistentGraph;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import ranker.TweetRanker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;


public class RankerDataServer {
	public static final int PORT = 4711;
	private static String name = "graph";
	private static String path = "../data/graph/";
	private HttpServer server;

	private class ComputeHandler implements HttpHandler {
		private PersistentGraph graph = null;
		private Random r = new Random();

		public ComputeHandler(PersistentGraph graph) {
			super();
			this.graph = graph;
		}

		public void handle(HttpExchange t) throws IOException {
			String response = "";
			Long gid = r.nextLong();
			if (gid.compareTo(0L) < 0) gid = -gid;

			TweetRanker ranker = new TweetRanker(new TemporaryGraph(graph));
			HashMap<Long,Double> pr = ranker.computePageRank();
			for(Map.Entry<Long, Double> entry : pr.entrySet()) {
				response = response + entry.getKey() + "\t" + entry.getValue() + "\n";
			}


			t.sendResponseHeaders(200, response.length());
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
		}
	}	

	/** This class handles the STOP requests. */
	private class StopHandler implements HttpHandler {
		private PersistentGraph graph;

		public StopHandler(PersistentGraph graph) {
			super();
			this.graph     = graph;
		}

		@Override
		public void handle(HttpExchange t) throws IOException {
			String response = "Closing...";
			t.sendResponseHeaders(200, response.length());
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
			server.stop(0);

			System.out.println("Saving data...");
			graph.store();
		}
	}

	/** This class handles the STATUS requests. */
	private class StatusHandler implements HttpHandler {
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

	public RankerDataServer(InetSocketAddress addr, int backlog, PersistentGraph graph) throws IOException {
		super();
		server = HttpServer.create(addr, backlog);
		server.createContext("/form", new FormHandler());
		server.createContext("/compute", new ComputeHandler(graph));
		server.createContext("/stop", new StopHandler(graph));
		server.createContext("/status", new StatusHandler(graph));
		server.createContext("/", new RequestHandler(graph));
		server.setExecutor(null);
	}

	public void start() {
		server.start();
	}

	public static void main(String[] args) {	
		try {
			PersistentGraph graph = new PersistentGraph(name, path);
			RankerDataServer dserver = new RankerDataServer(new InetSocketAddress(RankerDataServer.PORT), 15, graph);
			dserver.start();
			System.out.println("Ranker running...");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

/**
 * Dummy form used for debugging purposes.
 */
class FormHandler implements HttpHandler {
	public void handle(HttpExchange t) throws IOException {
		String response = "<html><b>Test form:</b><br />" +
		"<form method=\"post\" action=\"/\">" +
		"	<input type=\"text\" name=\"type\" /><br />" +
		"	<input type=\"text\" name=\"id\" /><br />" +
		"	<textarea name=\"refID\"></textarea><br />" +
		"	<textarea name=\"refID\"></textarea><br />" +
		"	<input type=\"submit\" />" +
		"</form></html>";
		t.sendResponseHeaders(200, response.length());
		OutputStream os = t.getResponseBody();
		os.write(response.getBytes());
		os.close();
	}
}

