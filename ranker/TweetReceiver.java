import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.HashMap;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;


public class TweetReceiver {
	
	public static final String CHARACTER_ENCODING = "UTF-8";
	public static final int PORT = 4711;
	
	public enum Type {
		RT("Retweet"),
		RP("Reply"),
		MN("Mention"),
		FW("Following"),
		TW("Tweets");
		
		private String value;
		
		Type(String value) {
			this.value = value;
		}
		
		public String getValue() {
			return value;
		}
	};
	
	public TweetReceiver(HttpServer server) {
		server.createContext("/form/", new FormHandler());
		server.createContext("/", new TweetHandler());
		server.start();
	}
	
	private boolean read(InputStream is, HashMap<String, String> parameters) throws Exception {
		InputStreamReader isr =
            new InputStreamReader(is, CHARACTER_ENCODING);
        BufferedReader br = new BufferedReader(isr);
        String query = br.readLine().toLowerCase();
        String[] parts = query.split("&");
        for (int i = 0; i < parts.length; i++) {
        	String[] parameter = parts[i].split("=");
        	if (parameter.length != 2) {
        		throw new IllegalArgumentException("Error for key: " + parameter[0]);
        	}	
        	parameter[0] = URLDecoder.decode(parameter[0], CHARACTER_ENCODING);
    		parameter[1] = URLDecoder.decode(parameter[1], CHARACTER_ENCODING);
        	parameters.put(parameter[0], parameter[1]);
        }
        return true;
	}
	
	private boolean parseParams(HashMap<String, String> param) {
		Type type = Type.valueOf(param.get("type").toUpperCase());
		String id = getParam(param, "id");
		String users = "";
		switch (type) {
			case RT:
			case RP:
				// RT or RP:
				String refID = getParam(param, "refid");
				break;
			case MN:
				users = getParam(param, "users");
				break;
			case FW:
				users = getParam(param, "users");
				break;
			case TW:
				String tweets = getParam(param, "tweets");
				break;
			default:
				throw new IllegalArgumentException("Unknown type \"" + type.name() + "\"");
		}
		return true;
	}
	
	private String getParam(HashMap<String, String> param, String key) {
		String val = param.get(key);
		if (val == null || val.isEmpty()) {
			throw new IllegalArgumentException("Missing required argument: " + key);
		}
		return val;
	}
	public static void main(String[] args) {
		HttpServer server = null;
		try {
			server = HttpServer.create(new InetSocketAddress(PORT), 0);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		new TweetReceiver(server);
	}
	
	class TweetHandler implements HttpHandler {
	    public void handle(HttpExchange t) throws IOException {
	        boolean success = false;
	        String response = "";
	        HashMap<String, String> parameters = new HashMap<String, String>();
	        try {
	        	if(read(t.getRequestBody(), parameters)) {
	        		success = parseParams(parameters);
	        	}
	        } catch (Exception e) {
	        	e.printStackTrace();
	        	response = e.getMessage();
	        }
	        if (success) {
	        	response = "<html><b>Success!</b></html>";
		        t.sendResponseHeaders(200, response.length());
	        } else {
	        	response = "<html><b>400: Bad request or internal error!</b><br /> " + response + "</html>";
		        t.sendResponseHeaders(400, response.length());	        	
	        }
	        OutputStream os = t.getResponseBody();
	        os.write(response.getBytes());
	        os.close();
	    }
	}
	class FormHandler implements HttpHandler {
	    public void handle(HttpExchange t) throws IOException {
        	String response = "<html><b>Test form:</b><br /><form method=\"post\" action=\"/\"><input type=\"text\" name=\"type\" /><br /><textarea name=\"t2\"></textarea><br /><input type=\"submit\" /></form></html>";
	        t.sendResponseHeaders(200, response.length());
	        OutputStream os = t.getResponseBody();
	        os.write(response.getBytes());
	        os.close();
	    }
	}
}
