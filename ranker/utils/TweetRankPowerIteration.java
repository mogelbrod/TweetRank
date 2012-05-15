package utils;

import java.util.ArrayList;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import graph.FilteredGraph;

public class TweetRankPowerIteration {	
	private static final Logger logger = Logger.getLogger("pi.logger");
	FilteredGraph graph;
	double ALPHA   = 0.20; 
	double BETA    = 0.40; 
	double GAMMA   = 0.20;
	double DELTA   = 0.04;
	double EPSILON = 0.16;

	TweetRankPowerIteration(FilteredGraph graph) {
		this.graph = graph;
	}

	/** Compute the euclidean distance between to vectors. */
	private static double EuclideanDistance(double [] v1, double[] v2) {
		double sum = 0.0;
		for(int i = 0; i < v1.length; ++i)
			sum += (v1[i]-v2[i])*(v1[i]-v2[i]);
		return Math.sqrt(sum);
	}	

	/** Compute the scalar product of two vectors. */
	private static double ScalarProduct(double [] v1, double [] v2) {
		double sum = 0.0;
		for(int i = 0; i < v1.length; ++i)
			sum += v1[i]*v2[i];
		return sum;
	}

	private double computeLij(Long ti, Long tj) {
		if ( !graph.hasReferences(ti) || !graph.getReference(ti).equals(tj)) return 0.0;
		return 1.0;
	}

	private double computeMij(Long ti, Long tj, Long uj) {
		if ( !graph.hasMentions(ti) || !graph.getMentions(ti).contains(uj) ) return 0.0;
		return 1.0/(graph.getMentions(ti).size() * graph.getTweetsByUser(uj).size()); 
	}

	private double computeFij(Long ui, Long uj) {
		if ( !graph.hasFriends(ui) || !graph.getFriends(ui).contains(uj) ) return 0.0;
		return 1.0/(graph.getFriends(ui).size() * graph.getTweetsByUser(uj).size());
	}

	private double computeHij(Long ti, Long tj) {
		if ( !graph.hasHashtags(ti) || !graph.hasHashtags(tj) ) return 0.0;
		HashSet<String> hti = graph.getHashtagsByTweet(ti);
		HashSet<String> htj = graph.getHashtagsByTweet(tj);
		Set<String> cht = utils.Functions.SetIntersection(hti,htj); // Common hashtags
		double H = 0.0;
		for ( String h : cht ) 
			H += 1.0/(hti.size()*graph.getTweetsByHashtag(h).size());
		return H;
	}

	double [] ComputeColumn(Long tj)
	{		
		int N = graph.getTweetSet().size();
		double Rij = 1.0 / N;
		double [] g = new double [N];

		Long uj = graph.getTweetAuthor(tj);
		for(int i = 0; i < N; ++i) {
			Long ti = graph.getTweetsList().get(i);
			Long ui = graph.getTweetAuthor(ti);

			double Lij = computeLij(ti, tj);
			double Mij = computeMij(ti, tj, uj);
			double Fij = computeFij(ui, uj);
			double Hij = computeHij(ti, tj);
			g[i] = (ALPHA*Rij + BETA*Lij + GAMMA*Mij + DELTA*Fij + EPSILON*Hij);

			// This is to ensure that the sum of the row is 1.0
			g[i] /= (ALPHA + BETA*(graph.hasReferences(ti) ? 1 : 0) + GAMMA*(graph.hasMentions(ti) ? 1 : 0) + 
					DELTA*(graph.hasFriends(ui) ? 1 : 0) + EPSILON*(graph.hasHashtags(ti) ? 1: 0));
		}

		return g;
	}

	public class PIColumnTask implements Callable<Boolean> {
		private int id;
		private int ntasks;
		private double[] xprev;
		private double[] x;

		PIColumnTask(int id, int ntasks, double [] xprev, double [] x) {
			this.id = id;
			this.ntasks = ntasks;
			this.xprev = xprev;
			this.x = x;
		}

		@Override
		public Boolean call() {
			for(int j = id; j < graph.getNumberOfTweets(); j += ntasks) {
				Long tw = graph.getTweetsList().get(j);
				double [] jv = ComputeColumn(tw);
				x[j] = ScalarProduct(xprev,jv);
			}
			return true;
		}
	}

	public Map<Long,Double> Compute(double precision) throws InterruptedException {
		double [] x = new double[graph.getNumberOfTweets()];
		double [] xprev = new double[graph.getNumberOfTweets()];
		double K = 1.0/graph.getNumberOfTweets();
		for(int i = 0; i < graph.getNumberOfTweets(); ++i) x[i] = K;

		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		ArrayList<PIColumnTask> tasks = new ArrayList<PIColumnTask>();
		int WORKER_THREADS = 8;

		double error = 10000;
		int iter = 0;
		do {
			double [] aux = xprev;
			xprev = x;
			x = aux;

			tasks.clear();
			for(int t = 0; t < WORKER_THREADS; ++t)
				tasks.add(new PIColumnTask(t, WORKER_THREADS, xprev, x));
			threadPool.invokeAll(tasks);

			error = EuclideanDistance(x, xprev); iter++;
			logger.debug("IT=" + iter + ", ERROR=" + error);
		} while (error > precision);
		
		tasks.clear();

		TreeMap<Long,Double> tr = new TreeMap<Long,Double>();
		for(int i = 0; i < graph.getNumberOfTweets(); ++i)
			tr.put(graph.getTweetsList().get(i), x[i]);
		
		threadPool.shutdown();
		return tr;
	}

	private static void showHelp() {
		System.out.println("TweetRankPowerIteration [-v graph_version] graph_path graph_prefix");
	}

	public static void main(String[] args) throws Throwable {
		String path = null;
		String prefix = null;
		Integer version = null;

		for(int i = 0; i < args.length-2; ++i) {
			if (args[i].equals("-h") || args[i].equals("--help")) {
				showHelp();
				return;
			} else if ( args[i].equals("-v") || args[i].equals("--version")  ) {
				version = Integer.parseInt(args[++i]);
			}
		}

		if (args.length < 2) {
			showHelp();
			return;
		}

		path = args[args.length-2];
		prefix = args[args.length-1];

		if (version == null)
			version = utils.Functions.graphLastVersion(path, prefix);
		
		BasicConfigurator.configure();
		logger.setLevel(Level.INFO);

		FilteredGraph graph = new FilteredGraph(path, prefix, version);
		System.out.println(graph.percentageTweetsWithHashtag());
		/*
		TweetRankPowerIteration pi = new TweetRankPowerIteration(graph);
		Map<Long,Double> pr = pi.Compute(1E-6);
		for(Map.Entry<Long, Double> e : pr.entrySet())
			System.out.println(e.getKey() + "=" + e.getValue());*/
	}
}
