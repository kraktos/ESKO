/**
 * 
 */
package code.dws.core.cluster.engine;

import gnu.trove.set.hash.THashSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.dto.PairDto;
import code.dws.utils.Constants;
import code.dws.utils.FileUtil;
import code.dws.utils.Utilities;
import code.dws.utils.Worker;
import code.dws.wordnet.SimilatityWebService;

/**
 * this class computes the pairwise scores for the given set of relation pairs,
 * OIE-OIE or KB-KB or OIE-KB
 * 
 * @author arnab
 */
public class ComputeSimilarity {

	/**
	 * logger
	 */
	// define Logger
	public static Logger logger = Logger.getLogger(ComputeSimilarity.class
			.getName());

	public static int TOPK_REV_PROPS = -1;

	private static final String DELIMIT = "\t";

	/*
	 * output location for the type pairs and the properties that are common
	 */

	private static List<Pair<String, String>> revbProps = null;

	private static Map<String, THashSet<ImmutablePair<String, String>>> ALL_PROPS = new HashMap<String, THashSet<ImmutablePair<String, String>>>();

	/**
     * 
     */
	public ComputeSimilarity() {
		//
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		String pairedRelationsFile = null;
		if (args.length != 2) {
			logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.cluster.engine.ReverbClusterProperty CONFIG.cfg <pairFile>");
		} else {
			Constants.loadConfigParameters(new String[] { "", args[0] });
			pairedRelationsFile = args[1];

			revbProps = FileUtil.readPairsOfProperties(args[1], DELIMIT);

			logger.info("Loaded " + revbProps.size() + " property pairs from "
					+ args[1]);

			System.out.println(new File(pairedRelationsFile).getName());

			// perform scoring
			doScoring(new File(pairedRelationsFile).getName());
		}

	}

	/**
	 * read the basic cluster pattern and try to improve the scoring
	 * 
	 * @param string
	 * 
	 * @throws IOException
	 */
	private static void doScoring(String string) throws IOException {
		String arg1 = null;
		String arg2 = null;
		PairDto resultPair = null;

		long cnt = 0;
		long starTime = 0;
		long cntr = 0;

		BufferedWriter writerRevWordnetSims = new BufferedWriter(
				new FileWriter(new File(Constants.OIE_DATA_PATH).getParent()
						+ "/all.trvb.wordnet.sim."
						+ Constants.SIMILARITY_FACTOR + ".csv"));

		long start = Utilities.startTimer();

		int cores = Runtime.getRuntime().availableProcessors();
		cores = (cores > Constants.THREAD_MAX_POOL_SIZE) ? cores
				: Constants.THREAD_MAX_POOL_SIZE;

		ExecutorService executorPool = Executors.newFixedThreadPool(cores);
		ExecutorCompletionService<PairDto> completionService = new ExecutorCompletionService<PairDto>(
				executorPool);

		// init task list
		List<Future<PairDto>> taskList = new ArrayList<Future<PairDto>>();

		// init http connection pool
		SimilatityWebService.init();

		try {

			for (Pair<String, String> pair : revbProps) {
				cnt++;

				arg1 = pair.getLeft();
				arg2 = pair.getRight();

				// add to the pool of tasks
				taskList.add(completionService.submit(new Worker(arg1, arg2)));

			}
			// shutdown pool thread
			executorPool.shutdown();

			logger.info("Pushed " + taskList.size() + " tasks to the pool ");
			starTime = System.currentTimeMillis();

			while (!executorPool.isTerminated()) {
				try {
					cntr++;
					Future<PairDto> futureTask = completionService.poll(
							Constants.TIMEOUT_MINS, TimeUnit.MINUTES);

					if (futureTask != null) {
						resultPair = futureTask.get();

						// write it out
						if (resultPair.getScore() > 0)
							writerRevWordnetSims.write(resultPair.getArg1()
									+ "\t"
									+ resultPair.getArg2()
									+ "\t"
									+ Constants.formatter.format(resultPair
											.getScore()) + "\n");
						if (cntr % 100000 == 0 && cntr > 100000) {
							Utilities.endTimer(start, 100
									* ((double) cntr / taskList.size())
									+ " percent done in ");
							writerRevWordnetSims.flush();
						}
					}

				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			writerRevWordnetSims.flush();
			writerRevWordnetSims.close();

			// init http connection pool
			SimilatityWebService.closeDown();
		}
	}
}