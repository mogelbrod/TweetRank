package httpserv;

import graph.MegaGraph;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Random;

import ranker.TweetRanker;

import com.larvalabs.megamap.MegaMapException;
import com.larvalabs.megamap.MegaMapManager;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;


public class RankerDataServer {
	public static final int PORT = 4711;
	private static String name = "graph";
	private static String path = "../data/graph/";
	private HttpServer server;

	private class StopHandler implements HttpHandler {
		private MegaMapManager MMmanager;
		private MegaGraph graph;

		public StopHandler(MegaMapManager MMmanager, MegaGraph graph) {
			this.MMmanager = MMmanager;
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
			graph.saveTweets();
			MMmanager.shutdown();
		}
	}

	public RankerDataServer(InetSocketAddress addr, int backlog, MegaGraph graph, MegaMapManager MMmanager) throws IOException {
		super();
		server = HttpServer.create(addr, backlog);
		server.createContext("/form", new FormHandler());
		server.createContext("/compute", new ComputeHandler(graph));
		server.createContext("/stop", new StopHandler(MMmanager, graph));
		server.createContext("/", new RequestHandler(graph));
		server.setExecutor(null);
	}

	public void start() {
		server.start();
	}

	public static void main(String[] args) {		
		MegaMapManager MMmanager = null;
		MegaGraph graph = null;
		RankerDataServer dserver = null;

		try {
			MMmanager = MegaMapManager.getMegaMapManager();
			MMmanager.setDiskStorePath(path);

			graph = MegaGraph.createMegaGraph(name, path);
			dserver = new RankerDataServer(new InetSocketAddress(RankerDataServer.PORT), 15, graph, MMmanager);
			dserver.start();
		} catch (Exception e) {
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

class ComputeHandler implements HttpHandler {
	private MegaGraph graph = null;
	private Random r = new Random();

	public ComputeHandler(MegaGraph graph) {
		super();
		this.graph = graph;
	}

	public void handle(HttpExchange t) throws IOException {
		Integer code = 400;
		String response = "";
		try {
			Long gid = r.nextLong();
			if (gid.compareTo(0L) < 0) gid = -gid;
			
			MegaGraph cgraph = graph.copy(gid.toString());
			TweetRanker ranker = new TweetRanker(cgraph);
			ranker.computePageRank();

			cgraph.delete();
			response = "OK!";
			code = 200;
		} catch (MegaMapException e) {
			e.printStackTrace();
			response = e.getMessage();
			code = 400;
		}
		
		t.sendResponseHeaders(code, response.length());
		OutputStream os = t.getResponseBody();
		os.write(response.getBytes());
		os.close();
	}
}