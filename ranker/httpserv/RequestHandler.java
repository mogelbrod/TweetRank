package httpserv;

import graph.PersistentGraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TimerTask;

import info.ziyan.net.httpserver.HttpContext;
import info.ziyan.net.httpserver.HttpHandler;
import info.ziyan.net.httpserver.HttpResponse;

import org.apache.log4j.Logger;

import utils.Time;

import computer.TweetRankComputer;

public class RequestHandler implements HttpHandler {
	private static final Logger logger = Logger.getLogger("ranker.logger");

	/** This method parses the body of a POST request and extracts the parameters. */
	public enum Type {
		// /RT (Retweet relationship) -> Tweet 'ID' is a retweet of tweet 'RefID'
		// /RP (Reply relationship)   -> Tweet 'ID' is a reply to tweet 'RefID'
		// /MN (Mention relationship) -> Tweet 'ID' mentions users in the list ['RefID']
		// /HT (Hashtags relationship)-> Tweet 'ID' mentions hashtags in the list ['RefID']
		// /FW (Follow relationship)  -> User 'ID' follows users in the list ['RefID']
		// /TW (Tweets relationship)  -> User 'ID' is the author of tweets in the list ['RefID']

		RETWEET("RT"),
		REPLY("RP"),
		MENTION("MN"),
		FOLLOWING("FW"),
		HASHTAG("HT"),
		TWEETS("TW");

		private String value;

		Type(String value) {
			this.value = value;
		}

		public static Type find(String search) {
			for (Type t : Type.values()) {
				if (t.value.equals(search)) {
					return t;
				}
			}
			throw new NullPointerException("No such type");
		}
	};

	private PersistentGraph graph;
	private TweetRankComputer trcomputer;
	private TimerTask trtask;

	public RequestHandler(PersistentGraph graph, TweetRankComputer trcomputer, TimerTask computask) {
		this.graph = graph;
		this.trcomputer = trcomputer;
		this.trtask = computask;
	}

	private static HashMap<String,ArrayList<String>> parseParams(String body_line) throws Exception {
		HashMap<String,ArrayList<String>> params = new HashMap<String,ArrayList<String>>();
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
		return params;
	}

	/** This method is used to send a Bad Reponse to the client. */
	private static void sendBadRequestResponse(HttpResponse response, String message) {
		try {
			logger.debug(RequestHandler.class.toString() + ": " + message);
			byte[] msg_bytes = message.getBytes();
			response.setCode(HttpResponse.HttpResponseCode.BAD_REQUEST);
			response.begin(msg_bytes.length);
			response.getBody().write(msg_bytes);
			response.end();
		} catch (Exception e) {
			logger.error(RequestHandler.class.toString() + ": Error sending response.", e);
		}
	}

	private static void sendServerErrorResponse(HttpResponse response, String message) {

	}

	/** This method is used to send a OK to the client. */
	private static void sendOKResponse(HttpResponse response, String message) {
		try {
			byte[] msg_bytes = message.getBytes();
			response.setCode(HttpResponse.HttpResponseCode.OK);
			response.begin(msg_bytes.length);
			response.getBody().write(msg_bytes);
			response.end();
		} catch (Exception e) {
			logger.error(RequestHandler.class.toString() + ": Error sending response.", e);
		}
	}

	private static class LineMessage {
		public Long line;
		public String message;
		public LineMessage(Long line, String message) {
			this.line = line;
			this.message = message;
		}
	}

	private static ArrayList<Long> convertStringToLongList(ArrayList<String> strList) throws Exception {
		ArrayList<Long> longList = new ArrayList<Long>(strList.size());
		for (String str : strList) {
			try { longList.add(Long.valueOf(str)); } 
			catch (Exception e) { throw new Exception("Failed to parse a REFID '" + str + "' as long."); }
		}
		return longList;
	}

	private void handleData(HttpContext context) {
		// Check POST request
		if ( !context.getRequest().getMethod().toUpperCase().equals("POST") ) {
			sendBadRequestResponse(context.getResponse(), "Only POST requests are valid.");
			return;
		}

		InputStreamReader isr;
		try {
			isr = new InputStreamReader(context.getRequest().getBody(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			sendBadRequestResponse(context.getResponse(), e.getMessage());
			return;
		}

		BufferedReader br = new BufferedReader(isr);
		ArrayList<LineMessage> exceptions = new ArrayList<LineMessage>(); 
		long line = 0;
		String body_line;
		try {
			// Process each line in the body... (Each line can contain a different request)
			while ( (body_line = br.readLine()) != null ) {
				line++; // New Line

				// Get action parameters
				HashMap<String, ArrayList<String>> params = null;
				try {
					params = parseParams(body_line);
				} catch (Exception e) {
					exceptions.add(new LineMessage(line, e.getMessage()));
					continue;
				}

				String typeStr = params.get("TYPE").get(0).toUpperCase();
				String idStr = params.get("ID").get(0);

				// Check the 'TYPE' parameter.
				Type type = null;
				type = Type.find(typeStr);
				if (type == null) {
					exceptions.add(new LineMessage(line, "Invalid TYPE '" + typeStr + "'."));
					continue;
				}

				// Check the 'ID' parameter.
				if ( idStr == null || params.get("ID").size() != 1) {
					exceptions.add(new LineMessage(line, "An unique ID parameter is mandatory."));
					continue;
				}

				// Parse 'ID' as Long
				Long id = null;
				try {
					id  = Long.valueOf(idStr);
				} catch (Exception e) {
					exceptions.add(new LineMessage(line, "Unable to parse ID '" + idStr + "' as long."));
					continue;
				}

				// Check the 'RefID' parameter.
				ArrayList<String> refIDs = params.get("REFID");
				if ( refIDs == null || refIDs.size() == 0 ) {
					exceptions.add(new LineMessage(line, "At least one 'REFID' parameter must be indicated."));
					continue;
				}

				// Parse 'RefID' as Long
				ArrayList<Long> refLongIDs = null;
				if (type != Type.HASHTAG) {
					try {
						refLongIDs = convertStringToLongList(refIDs);
					} catch (Exception e) {
						exceptions.add(new LineMessage(line, e.getMessage()));
						continue;
					}
				}

				try {
					if (type == Type.RETWEET || type == Type.REPLY) {
						if (refLongIDs.size() != 1) {
							exceptions.add(new LineMessage(line, "For RT and RP only one REFID is allowed. Size: " + refLongIDs.size()));
							continue;
						}
						logger.debug("ADD " + type + ": "+ id + " " + refLongIDs.get(0));
						graph.addRefTweets(id, refLongIDs.get(0));
					} else if (type == Type.FOLLOWING) {
						logger.debug("ADD " + type + ": "+ id + " " + refLongIDs);
						graph.addFriends(id, refLongIDs);
					} else if (type == Type.MENTION) {
						logger.debug("ADD " + type + ": "+ id + " " + refLongIDs);
						graph.addMentioned(id, refLongIDs);
					} else if (type == Type.TWEETS) {
						logger.debug("ADD " + type + ": "+ id + " " + refLongIDs);
						graph.addUserTweets(id, refLongIDs);
					} else if (type == Type.HASHTAG) {
						logger.debug("ADD " + type + ": "+ id + " " + refIDs);
						graph.addHashtags(id, refIDs);
					} else {
						exceptions.add(new LineMessage(line, "Unknown type: " + type.toString()));
						continue;
					}
				} catch (Exception e) {
					exceptions.add(new LineMessage(line, "Internal error handling request."));	
				}
			}
		} catch (IOException e) {
			sendBadRequestResponse(context.getResponse(), e.getMessage());
		}

		if ( exceptions.size() > 0 ) {
			String errorMessages = "";
			for(LineMessage lm : exceptions)
				errorMessages += "Error processing line " + lm.line + ": " + lm.message;
			sendBadRequestResponse(context.getResponse(), errorMessages); // Send Bad response.
		} else sendOKResponse(context.getResponse(), "OK!"); // Send OK response.		
	}

	private void handleStatus(HttpContext context) {
		try {
			String response = "Persistent graph info:\n======================\n" +
			"Number of tweets: " + graph.getNumberOfTweets() + "\n" + 
			"Number of users: " + graph.getNumberOfUsers() + "\n" +
			"Number of hashtags: " + graph.getNumberOfHashtags() + "\n" +
			"Average tweets per user: " + graph.getAverageTweetsPerUser() + "\n" +
			"Average friends per user: " + graph.getAverageFriendsPerUser() + "\n" +
			"Average references per tweet: " + graph.getAverageReferencePerTweet() + "\n" +
			"Average mentions per tweet: " + graph.getAverageMentionsPerTweet() + "\n" +
			"Average hashtags per tweet: " + graph.getAverageHashtagsPerTweet() + "\n\n";

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
				"Average friends per user: " + trcomputer.getTemporaryGraph().getAverageFriendsPerUser() + "\n" +
				"Average references per tweet: " + trcomputer.getTemporaryGraph().getAverageReferencePerTweet() + "\n" +
				"Average mentions per tweet: " + trcomputer.getTemporaryGraph().getAverageMentionsPerTweet() + "\n" +
				"Average hashtags per tweet: " + trcomputer.getTemporaryGraph().getAverageHashtagsPerTweet() + "\n\n";
			} else {
				response += "Temporary graph not initialized.\n\n";
			}
			
			response += "Last computation: " + (enddate == null ? "Never" : Time.formatDate("yyyy/MM/dd HH:mm:ss", enddate)) + "\n";
			/*if ( state == TweetRankComputer.State.WORKING ) {
				response += "Approximate percentage of completion: " + trcomputer.getExpectedPercentageOfCompletion()*100 + "\n" +
				"Approximate remaining time: " + trcomputer.getExpectedRemainingTime() + "\n";
			}*/
			if ( elapsed != null )	response += "Elapsed time: " + elapsed;


			sendOKResponse(context.getResponse(), response);
		} catch ( Throwable th ) {
			logger.error("Error during status recopilation.", th);
			sendServerErrorResponse(context.getResponse(), "");
		}	
	}
	
	private void handleCompute(HttpContext context) {
		Thread th = new Thread(trtask);
		th.start();
		sendOKResponse(context.getResponse(), "TweetRank computation started!");
	}

	/** This method is executed when a new HTTP connection is accepted
	by the server. */
	public void handle(HttpContext context) {
		if ( context.getRequest().getPath().equals("/") ) {
			handleData(context);
		} else if ( context.getRequest().getPath().equals("/status") ) {
			handleStatus(context);
		} else if ( context.getRequest().getPath().equals("/compute") ) {
			handleCompute(context);
		} else {
			sendBadRequestResponse(context.getResponse(), "Unknown context.");
		}
	}
}