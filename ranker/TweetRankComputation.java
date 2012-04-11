
import java.util.*;

public class TweetRankComputation {
    private double ALPHA = 0.15;           // 0.150
    private double BETA  = 0.50;           // 0.425
    private double GAMMA = 0.30;           // 0.255
    private double DELTA = (1-BETA-GAMMA); // 0.170
	

    Hashtable<String, HashSet<String>> L; // tweets and retweets
    Hashtable<String, HashSet<String>> M; // mentions
    Hashtable<String, HashSet<String>> F; // followers

    Hashtable<String,Integer> counter = new Hashtable<String,Integer>();

    final public Hashtable<String,Double> MCCompletePath(Set<String> docs) {	
	Random r = new Random();
	while(1) { // This should run continuously
	    String d = ChooseRandomWebpage(docs);
	    while(r.nextDouble() > ALPHA) {
		
		double p = r.nextDouble();
		HashSet<String> hs = null;
		if ( p < BETA ) { 
		    hs = L.get(d);
		} else if ( p < GAMMA ) {
		    hs = M.get(d);
		} else { // p < DELTA
		    hs = F.get(d);
		}
		
		if(hs == null || hs.size() == 0) d = ChooseRandomWebpage(docs);
		else {
		    String [] nps = hs.toArray(String [0]);
		    d = nps[r.nextInt(nps.length)];
		}
	    }
	    counter[d]++;
	}
    }
}