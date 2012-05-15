import graph.PersistentGraph;
import computer.TweetRankComputer;

import httpserv.RequestHandler;
import info.ziyan.net.httpserver.HttpServer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class TweetRanker {
	private static final Logger logger = Logger.getLogger("ranker.logger");
	public static final int PORT = 4711;

	private static String prefix = "graph";
	private static String path = "../data/graph/";
	private static String RankingName = "../data/tweetrank.tr";
	//private static String RankingName = "/home/ir12/apache-solr-3.6.0/example/solr/data/external_rank";
	private static long RankingPeriod = MinToMilli(120);
	private static long StoringPeriod = MinToMilli(30);

	private HttpServer server;
	private Timer rankerTimer = new Timer();
	private Timer storeTimer = new Timer();
	private PersistentGraph graph;
	private TweetRankComputer ranker;
	private RankingComputationTask computask = new RankingComputationTask();

	/** Converts minutes to milliseconds. */
	private static long MinToMilli(long min) { return min*60000; }

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
			Integer version = utils.Functions.graphLastVersion(path, prefix) + 1;
			this.graph.store(path, prefix, version);
		}
	}

	/** Periodic TweetRank computation task. */
	private class RankingComputationTask extends TimerTask {

		private void notifySolr () {
			try {
				URL url = new URL("http://176.9.149.66:8983/solr/reloadCache");
				url.getContent();
			} catch (Exception e) {
				logger.error("Error trying to notify solr about the new TweetRank", e);
			}
		}

		@Override
		public void run() {
			try {
				// Start computation!
				TreeMap<Long,Double> pr = ranker.compute(graph.createTemporaryGraph());

				// If everything was OK, save the result on a file
				if ( pr != null ) {
					logger.info("Saving TweetRank file...");
					PrintWriter pwriter = new PrintWriter(new FileWriter(RankingName));
					for(Map.Entry<Long, Double> entry : pr.entrySet())
						pwriter.println(entry.getKey() + "=" + entry.getValue());
					pwriter.close();
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
			int nextVersion = utils.Functions.graphLastVersion(path, prefix) + 1;
			graph.store(path, prefix, nextVersion);
		}
	}

	public TweetRanker(PersistentGraph graph) throws IOException {
		super();
		this.graph = graph;
		this.ranker = new TweetRankComputer();
		server = new HttpServer(TweetRanker.PORT,  new RequestHandler(this.graph, this.ranker, this.computask));
	}

	public void start() {
		server.start();
		//rankerTimer.schedule(this.computask, RankingPeriod, RankingPeriod);
		storeTimer.schedule(new PersistentStoreTask(), StoringPeriod, StoringPeriod);
	}

	public void stop() {
		rankerTimer.cancel();
		storeTimer.cancel();
	}

	public static void main(String[] args) {
		PersistentGraph graph = null;
		TweetRanker server = null;
		BasicConfigurator.configure();
		logger.setLevel(Level.INFO);

		try {
			Integer version = utils.Functions.graphLastVersion(path, prefix);
			graph  = new PersistentGraph(path, prefix, version);
			server = new TweetRanker(graph);
			Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownThread(server, graph)));
		} catch (Throwable e) {
			logger.fatal("Error on initialization.", e);
		}

		logger.info("Ranker ready!");
		server.start();
	}
}

