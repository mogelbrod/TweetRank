import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.HashMap;
import java.net.URI;
import java.util.ArrayList;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

class RequestHandler implements HttpHandler {
    /** This method parses the body of a POST request and extracts the parameters. */
    private HashMap<String,ArrayList<String>> parseParams(InputStream is) throws Exception {
	HashMap<String,ArrayList<String>> params = new HashMap<String,ArrayList<String>>(); 
	InputStreamReader isr = new InputStreamReader(is, "UTF-8");
        BufferedReader br = new BufferedReader(isr);
	String body_line;

	while ( (body_line = br.readLine()) != null ) {
	    body_line = body_line.toLowerCase();
	    String[] parts = body_line.split("&");
	    for(int i = 0; i < parts.length; ++i) {
		String[] parameter = parts[i].split("=");
		if (parameter.length != 2 || parameter[0].length() == 0 || parameter[1].length() == 0)
		    throw new Exception("Bad parameters.");

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
	boolean success = false;
	String response = "";

	// Check POST request
	if ( !t.getRequestMethod().toUpperCase().equals("POST") ) {
	    sendBadRequestResponse(t, "Only POST requests valid.");
	    return;
	}

	// Check action 
	// /RT (Retweet relationship) -> Tweet 'ID' is a retweet of tweet 'RefID'
	// /RP (Reply relationship)   -> Tweet 'ID' is a reply to tweet 'RefID'
	// /MN (Mention relationship) -> Tweet 'ID' mentions users in the list ['RefID']
	// /FW (Follow relationship)  -> User 'ID' follows users in the list ['RefID']
	// /TW (Tweets relationship)  -> User 'ID' is the author of tweets in the list ['RefID']
	String action = t.getRequestURI().toString().toUpperCase();
	if ( !action.equals("/RT") && !action.equals("/RP") && !action.equals("/MN") &&
	     !action.equals("/FW") && !action.equals("/TW") ) {
	    sendBadRequestResponse(t, "Only [RT|RP|MN|FW|TW] actions valid.");
	    return;
	}

	// Get action parameters
	HashMap<String, ArrayList<String>> params = null;
	try {
	    params = parseParams(t.getRequestBody());
	} catch (Exception e) {
	    sendBadRequestResponse(t, "Bad parameters.");
	    return;
	}

	ArrayList<String> ID_values = params.get("ID");
	ArrayList<String> RefID_values = params.get("REFID");

	// Check the 'ID' param.
	if ( ID_values == null || ID_values.size() != 1 ) {
	    sendBadRequestResponse(t, "An unique 'ID' parameter is mandatory.");
	    return;
	}
	
	// Check the 'RefID' param.
	if ( RefID_values == null || RefID_values.size() == 0 ) {
	    sendBadRequestResponse(t, "At least one 'REFID' parameter must be indicated.");
	    return;
	}

	if ( action.equals("/RT") ) {
	    //String tweet_id = ID_values.get(0);
	    //ArrayList<String> ref_tweet_ids = RefID_values;
	} else if ( action.equals("/RP") ) {
	    //String tweet_id = ID_values.get(0);
	    //ArrayList<String> ref_tweet_ids = RefID_values;
	} else if ( action.equals("/MN") ) {
	    //String tweet_id = ID_values.get(0);
	    //ArrayList<String> ref_user_ids = RefID_values;
	} else if ( action.equals("/FW") ) {
	    //String user_id = ID_values.get(0);
	    //ArrayList<String> ref_user_ids = RefID_values;
	} else {
	    //String user_id = ID_values.get(0);
	    //ArrayList<String> ref_tweet_ids = RefID_values;
	}

	// Send OK response.
	sendOKResponse(t, "");
    }
}