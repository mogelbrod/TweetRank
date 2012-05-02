package httpserv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import ranker.TweetRanker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

class RequestHandler implements HttpHandler {
	/** This method parses the body of a POST request and extracts the parameters. */

	public enum Type {
		// /RT (Retweet relationship) -> Tweet 'ID' is a retweet of tweet 'RefID'
		// /RP (Reply relationship)   -> Tweet 'ID' is a reply to tweet 'RefID'
		// /MN (Mention relationship) -> Tweet 'ID' mentions users in the list ['RefID']
		// /HT (Hashtags relationship)-> Tweet 'ID' mentions hashtags in the list ['RefID']
		// /FW (Follow relationship)  -> User 'ID' follows users in the list ['RefID']
		// /TW (Tweets relationship)  -> User 'ID' is the author of tweets in the list ['RefID']

		RT("Retweet"),
		RP("Reply"),
		MN("Mention"),
		FW("Following"),
		HT("Hashtag"),
		TW("Tweets");

		private String value;

		Type(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}
	};
	
	private TweetRanker ranker;
	
	public RequestHandler(TweetRanker ranker) {
		this.ranker = ranker;
	}

	private HashMap<String,ArrayList<String>> parseParams(InputStream is) throws Exception {
		HashMap<String,ArrayList<String>> params = new HashMap<String,ArrayList<String>>(); 
		InputStreamReader isr = new InputStreamReader(is, "UTF-8");
		BufferedReader br = new BufferedReader(isr);
		String body_line;

		while ( (body_line = br.readLine()) != null ) {
			String[] parts = body_line.split("&");
			for(int i = 0; i < parts.length; ++i) {
				String[] parameter = parts[i].split("=");
				if (parameter.length != 2 || parameter[0].length() == 0 || parameter[1].length() == 0)
					throw new Exception("Bad parameter for key '" + parameter[0] + "'.");
				parameter[0] = parameter[0].toUpperCase();
				ArrayList<String> values = params.get(parameter[0]);
				if ( values == null ) values = new ArrayList<String>();
				values.add(parameter[1]);
				params.put(parameter[0], values);
			}
		}
		return params;
	}

	/** This method is used to send a Bad Reponse to the client. */
	private void sendBadRequestResponse(HttpExchange t, String message) {
		try {
			System.err.println(message);
			t.sendResponseHeaders(400, message.length());
			t.getResponseBody().write(message.getBytes());
			t.getResponseBody().close();
		} catch (Exception e) {
		}
	}

	/** This method is used to send a OK to the client. */
	private void sendOKResponse(HttpExchange t, String message) {
		try {
			t.sendResponseHeaders(200, message.length());
			t.getResponseBody().write(message.getBytes());
			t.getResponseBody().close();
		} catch (Exception e) {
		}
	}

	/** This method is executed when a new HTTP connection is accepted
	by the server. */
	public void handle(HttpExchange t) throws IOException {

		// Check POST request
		if ( !t.getRequestMethod().toUpperCase().equals("POST") ) {
			sendBadRequestResponse(t, "Only POST requests valid.");
			return;
		}

		// Get action parameters
		HashMap<String, ArrayList<String>> params = null;
		try {
			params = parseParams(t.getRequestBody());
		} catch (Exception e) {
			sendBadRequestResponse(t, "Bad parameters. " + e.getMessage());
			e.printStackTrace();
			return;
		}
		
		String typeStr = params.get("TYPE").get(0).toUpperCase();
		String idStr = params.get("ID").get(0);

		Type type = null;
		try {
			 type = Type.valueOf(typeStr);
		} catch (Exception e) {
			sendBadRequestResponse(t, "Invalid type '" + typeStr + "'.");
			return;
		}
		// Check the 'ID' parameter.

		if ( idStr == null || params.get("ID").size() != 1) {
			sendBadRequestResponse(t, "An unique 'ID' parameter is mandatory.");
			return;
		}
		
		Long id = null;
		try {
			id  = Long.valueOf(idStr);
		} catch (Exception e) {
			sendBadRequestResponse(t, "Unable to parse id '" + idStr + "' as long.");
			return;
		}
		

		ArrayList<String> refIDs = params.get("REFID");

		// Check the 'RefID' parameter.
		if ( refIDs == null || refIDs.size() == 0 ) {
			sendBadRequestResponse(t, "At least one 'REFID' parameter must be indicated.");
			return;
		}
		
		ArrayList<Long> refLongIDs = new ArrayList<Long>(refIDs.size());
		if (type != Type.HT) {
			for (String refID : refIDs) {
				try {
					refLongIDs.add(Long.valueOf(refID));
				} catch (Exception e) {
					sendBadRequestResponse(t, "Failed to parse a refID '" + refID + "' as long.");
					return;
				}
			}
		}
		
		if (type == Type.RT || type == Type.RP) {
			if (refLongIDs.size() != 1) {
				sendBadRequestResponse(t, "For RT and RP only one refID is allowed. Size: " + refLongIDs.size());
				return;
			}
			ranker.addRefTweets(id, refLongIDs.get(0));
		} else if (type == Type.FW) {
			ranker.addFollows(id, refLongIDs);
		} else if (type == Type.MN) {
			ranker.addMentioned(id, refLongIDs);
		} else if (type == Type.TW) {
			ranker.addUserTweets(id, refLongIDs);
		} else if (type == Type.HT) {
			ranker.addHashtags(id, refIDs);
		} else {
			sendBadRequestResponse(t, "Unknown type: " + type.toString());
			return;
		}

		// Send OK response.
		sendOKResponse(t, "OK!");
	}
}