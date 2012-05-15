package computer;

import graph.TemporaryGraph;
import utils.Time;

import java.util.*;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class TweetRankComputer {
	private static final Logger logger = Logger.getLogger("ranker.logger");	

	private static final int NUM_WORK_THREADS = 16;
	ExecutorService threadPool = Executors.newFixedThreadPool(NUM_WORK_THREADS);
	ArrayList<ComputationTask> tasks = new ArrayList<ComputationTask>(NUM_WORK_THREADS);

	private ReentrantLock cLock = new ReentrantLock(); // Avoids concurrent TweetRank computation instances

	/** Read-only graph used to compute the TweetRank. */
	private TemporaryGraph graph = null;
	private int M = 100;

	public static enum State {
		WORKING, IDLE
	}

	private State state = State.IDLE;
	private Date StartEndDate[] = new Date[2];

	/**
	 * This exception is thrown when a TweetRank computation attemps to start when
	 * an other has not finished yet.
	 */
	public static class ConcurrentComputationException extends Exception {
		private static final long serialVersionUID = 6110756217026832483L;
	}

	/**
	 * This exception is thrown when a TweetRank computation attemps to start
	 * but the Temporary Graph has not been initialized.
	 */
	public static class NullTemporaryGraphException extends Exception {
		private static final long serialVersionUID = -7656094713092868726L;
	}


	/**
	 * Starts the computation of the TweetRank.
	 * @return A HashMap where each entry is a pair (TweetID, TweetRank). If any problem
	 * ocurred, the result is null.
	 * @throws ConcurrentComputationException is thrown if there is a concurrent computation.
	 * @throws NullTemporaryGraphException if the graph was not initialized.
	 */
	public TreeMap<Long,Double> compute(TemporaryGraph graph) 
	throws ConcurrentComputationException, NullTemporaryGraphException 
	{
		TreeMap<Long,Double> tweetrank = null;

		// Check if there is another thread already computing the tweetrank
		if ( !cLock.tryLock() ) 
			throw new ConcurrentComputationException();

		// Check if the graph is null
		if ( graph == null ) {
			cLock.unlock();
			throw new NullTemporaryGraphException();
		}

		try {
			// Set the temporary graph
			this.graph = graph;

			// Determine the path length to be used
			M = graph.getTweetList().size()/100;
			if (M < 100) M = 100;

			// Start computation!
			tweetrank = MCCompletePathStopDanglingNodes();
		} finally {
			cLock.unlock();
		}

		return tweetrank;
	}	

	/** 
	 * Monte Carlo method that computes an approximation to TweetRank.
	 * Multiple threads are created to perform the computation and improve its execution time.
	 * @return A HashMap where each entry is a pair (TweetID, TweetRank). If any problem
	 * ocurred, the result is null.
	 */
	private TreeMap<Long,Double> MCCompletePathStopDanglingNodes() {	
		TreeMap<Long,Double> tweetrank = null;
		
		// Work started...
		state = State.WORKING;
		StartEndDate[0] = new Date();
		logger.info("Ranking started at " + Time.formatDate("yyyy/MM/dd HH:mm:ss", StartEndDate[0]));

		tasks.clear();
		for(int task_id = 0; task_id < NUM_WORK_THREADS; ++task_id) 
			tasks.add(new ComputationTask(task_id, NUM_WORK_THREADS, M, graph));
		
		try {
			// Schedule tasks
			List<Future<HashMap<Long,Long>>> future_results = threadPool.invokeAll(tasks);
			
			// Get partial results
			ArrayList<HashMap<Long,Long>> completed_results = new ArrayList<HashMap<Long,Long>>(); 
			for( Future<HashMap<Long,Long>> result : future_results )
				completed_results.add(result.get());
			
			// Merge results
			tweetrank = MergeAndNormalizeCounters(completed_results, 10L);
		} catch (InterruptedException e) {
			logger.error("Worker interruped while computing TweetRank.", e);
		} catch (ExecutionException e) {
			logger.error("TweetRank computation failed.", e);
		}

		// Work finished!
		StartEndDate[1] = new Date();
		state = State.IDLE;
		logger.info("Ranking finished at " + Time.formatDate("yyyy/MM/dd HH:mm:ss", StartEndDate[1]));
		
		return tweetrank;
	}

	/**
	 * This method merges and normalizes a collection of counters. The sum of all the values in the
	 * result HashMap sums 1.0. 
	 * For example, suppose that visitCounters is a collection like { [(1,2), (2,4), (3,1)], [(1,4), (2,3), (4,2)] }
	 * The merged and normalized result would be then [(1,6.0/16.0),(2,7.0/16.0),(3,1.0/16.0),(4,2.0/16.0)]
	 * @param visitCounters Collection of counters to merge and normalize.
	 * @return Returns a merged and normalized HashMap, so that the sum of all values is 1.0.
	 */
	private static TreeMap<Long,Double> MergeAndNormalizeCounters(Collection<HashMap<Long,Long>> visitCounters, Long MaxRange) 
	{
		// Merge all the counters
		TreeMap<Long,Long> merge = new TreeMap<Long,Long>();
		Long sum = 0L;
		Long min = null;
		Long max = null;

		for(HashMap<Long,Long> counter : visitCounters) {
			for(Entry<Long,Long> entry : counter.entrySet()) {
				Long c = merge.get(entry.getKey());
				if ( c == null )  c = entry.getValue();
				else c = c + entry.getValue();
				sum += entry.getValue();
				if ( min == null || min.compareTo(c) > 0 ) min = new Long(c);
				if ( max == null || max.compareTo(c) < 0 ) max = new Long(c);
				merge.put(entry.getKey(), c);
			}
		}


		logger.debug("min=" + min.toString() + ", max=" + max.toString() + ", maxRange=" + MaxRange.toString());

		// Normalize the counters
		TreeMap<Long,Double> norm = new TreeMap<Long,Double>();

		for(Entry<Long,Long> entry : merge.entrySet()) {
		    Double val = MaxRange * entry.getValue()/(double)max;
		    logger.debug("id="+entry.getKey()+", oval=" + entry.getValue() + ", nval=" + val);
		    norm.put(entry.getKey(),  val);
		}

		return norm;
	}

	/**
	 * Returns the current temporary graph being used. The graph should
	 * not be manipulated with write operations.
	 * @return Number of tweets in the temporary graph.
	 */
	public TemporaryGraph getTemporaryGraph() {
		return graph;
	}	

	/**
	 * Returns the state of the TweetRank computer. WORKING will be returned when the 
	 * computation is active and IDLE when it is not.
	 * WARNING: NOT THREAD-SAFE! 
	 * @return State of the TweetRankComputer.
	 */
	public State getState() {
		return state;
	}

	/**
	 * Returns the elapsed time for the last started TweetRank computation.
	 * WARNING: NOT THREAD-SAFE! 
	 * @return If a computation is ongoing, returns the elapsed time since its beginning,
	 * if the status is IDLE and a computation was completed, returns the elapsed time of
	 * the previous computation, otherwise returns null.
	 */
	public Time getElapsedTime() {
		if ( state == State.WORKING ) {
			return new Time((new Date()).getTime() - StartEndDate[0].getTime());
		} else if ( StartEndDate[1] != null && StartEndDate[1].compareTo(StartEndDate[0]) > 0 ) {
			return new Time(StartEndDate[1].getTime() - StartEndDate[0].getTime());
		} else {
			return null;
		}
	}

	/**
	 * Returns the end date of the last TweetRank computation.
	 * WARNING: NOT THREAD-SAFE! 
	 * @return Date of the last computation.
	 */
	public Date getEndDate() {
		return StartEndDate[1];
	}

	/**
	 * Returns the percentage of completion of the TweetRank computation.
	 * WARNING: NOT THREAD-SAFE! 
	 * @return If the computation is active, returns the percentage of completion of the TweetRank computation.
	 * Otherwise returns 0.
	 */
	public double getExpectedPercentageOfCompletion() {
		/*if ( state == State.IDLE ) return 0.0;

		double expected_length = 1/ComputationTask.BORED_PROBABILITY; // Expected length
		double dev_length = Math.sqrt(3.0)/6.0;// Standard deviation in the length

		double ExpectedVisits = graph.getNumberOfTweets()*M*expected_length;
		long CurrentVisits = 0L;

		for(ComputationTask ct : tasks)
			CurrentVisits += ct.getTotalCounter();

		while(CurrentVisits > ExpectedVisits)
			ExpectedVisits += dev_length;

		return CurrentVisits/ExpectedVisits;*/
		return 0.0;
	}

	/**
	 * Returns the expected remaining time for the completion of the ongoing
	 * TweetRank computation (or zero, if state is IDLE).
	 * WARNING: NOT THREAD-SAFE! 
	 * @return Expected remaining time.
	 */
	public Time getExpectedRemainingTime() {
		if ( state == State.IDLE ) return new Time(0);

		double completed = getExpectedPercentageOfCompletion();
		if (completed < 1E-5) return new Time();

		long elapsed = (new Date()).getTime() - StartEndDate[0].getTime();
		return new Time((long)(elapsed/completed) - elapsed);
	}	
}
