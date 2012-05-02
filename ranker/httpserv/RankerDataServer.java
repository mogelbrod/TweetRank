package httpserv;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import ranker.TweetRanker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;


public class RankerDataServer {
	public static final int PORT = 4711;
	private HttpServer server;
	private InetSocketAddress addr;
	private int backlog;
	private TweetRanker ranker;

	public RankerDataServer(InetSocketAddress addr, int backlog, TweetRanker ranker) throws IOException {
		super();
		this.addr = addr;
		this.backlog = backlog;
		this.ranker = ranker;

		server = HttpServer.create(addr, backlog);
		server.createContext("/form", new FormHandler());
		server.createContext("/", new RequestHandler(ranker));
		server.setExecutor(null);
	}

	public void start() {
		server.start();
	}

	public static void main(String[] args) {
		try {
			RankerDataServer server = new RankerDataServer(new InetSocketAddress(PORT), 0, new TweetRanker());
			server.start();
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
