

import graph.TemporaryGraph;
import graph.PersistentGraph;
import httpserv.RequestHandler;
import httpserv.StatusHandler;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import com.sun.net.httpserver.HttpServer;
import computer.TweetRankComputer;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class TweetRanker {
	private static final Logger logger = Logger.getLogger("ranker.logger");
	public static final int PORT = 4711;

	private static String name = "graph";
	private static String path = "../data/graph/";
	//private static String RankingName = "../data/tweetrank.tr";
	private static String RankingName = "/home/ir12/apache-solr-3.6.0/example/solr/data";
	private static long RankingPeriod = MinToMilli(60);  
	private static long StoringPeriod = MinToMilli(20);

	private HttpServer server;
	private Timer rankerTimer = new Timer();
	private Timer storeTimer = new Timer();
	private PersistentGraph graph;
	private TweetRankComputer ranker;

	/** Converts minutes to milliseconds. */
	private static long MinToMilli(long min) { return min*60000; }

	/** Rename a file. */
	/*private static boolean renameFile(String file, String toFile) {
		java.io.File toBeRenamed = new java.io.File(file);
		if (!toBeRenamed.exists() || toBeRenamed.isDirectory()) return false;
		java.io.File newFile = new java.io.File(toFile);
		return toBeRenamed.renameTo(newFile);
	}

	private static String generateTemporalFilename() {
		return java.util.UUID.randomUUID().toString();
	}*/

	/** Shutdown hook. Saves the persistent information on disk. */
	private static class ShutdownThread implements Runnable {
		TweetRanker server;
		PersistentGraph graph;

		public ShutdownThread(TweetRanker server, PersistentGraph graph) {
			super();
			this.server = server;
			this.graph = graph;
		}

		@Override
		public void run() {
			logger.info("Closing...");
			this.server.stop();
			this.graph.store();
		}		
	}

	/** Periodic TweetRank computation task. */
	private class RankingComputationTask extends TimerTask {
		
		private void notifySolr () {
			try {
				URL url = new URL("http://176.9.149.66:8983/solr/reloadCache");
				url.getContent();
			} catch (Exception e) {
				logger.error("Error while notifying solr about the new TweetRank", e);
			}
		}
		
		@Override
		public void run() {
			try {
				ranker.setTemporaryGraph(new TemporaryGraph(graph));  // Creates a new temporary graph 
				TreeMap<Long,Double> pr = ranker.compute();           // Start computation!
				if ( pr != null ) { // If everything was OK, save the result on a file
					logger.info("Saving TweetRank to temporal file...");
					//String tmpFile = RankingName + "_" + generateTemporalFilename();
					
					PrintWriter pwriter = new PrintWriter(new FileWriter(RankingName));
					for(Map.Entry<Long, Double> entry : pr.entrySet())
						pwriter.println(entry.getKey() + "=" + 100.0*entry.getValue());
					pwriter.close();
					/*if ( !renameFile(tmpFile, RankingName) ) 
						logger.error("Error moving the temporal file to the final location.");*/
					notifySolr();
				}
			} catch (TweetRankComputer.ConcurrentComputationException e) {
				logger.info("A TweetRank computation is already ongoing.");
			} catch (Throwable t) {
				logger.error("Error during the TweetRank computation.", t);
			}
		}
	}

	private class PersistentStoreTask extends TimerTask {
		@Override
		public void run() {
			logger.info("Storing persistent graph...");
			graph.store();
		}
	}

	public TweetRanker(InetSocketAddress addr, int backlog, PersistentGraph graph) throws IOException {
		super();
		this.graph = graph;
		this.ranker = new TweetRankComputer();
		server = HttpServer.create(addr, backlog);
		server.createContext("/status", new StatusHandler(this.graph, this.ranker));
		server.createContext("/", new RequestHandler(graph));
		server.setExecutor(null);
	}

	public void start() {
		server.start();
		rankerTimer.schedule(new RankingComputationTask(), RankingPeriod, RankingPeriod);
		storeTimer.schedule(new PersistentStoreTask(), StoringPeriod, StoringPeriod);
	}

	public void stop() {
		server.stop(0);
		rankerTimer.cancel();
		storeTimer.cancel();
	}

	public static void main(String[] args) {
		PersistentGraph graph = null;
		TweetRanker server = null;
		BasicConfigurator.configure();
		logger.setLevel(Level.INFO);

		try {	
			graph  = new PersistentGraph(name, path);
			server = new TweetRanker(new InetSocketAddress(TweetRanker.PORT), 15, graph);
			Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownThread(server, graph)));
		} catch (Throwable e) {
			logger.fatal("Error on initialization.", e);
		}

		logger.info("Ranker ready!");
		server.start();
	}
}

