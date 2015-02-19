/**
 * 
 */
package code.dws.core.cluster.analysis;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.utils.Constants;

/**
 * Often the final output clusters can be bit still coarse. Use this class to
 * further find sub clusters within a larger coarse cluster. We want a cluster
 * with average sized relations not one with 100 in them !!
 * 
 * 
 * @author adutta
 *
 */
public class ClusterOptimizer {

	// define Logger
	public static Logger logger = Logger.getLogger(ClusterOptimizer.class
			.getName());

	private static int lastSize = 0;
	public static Map<Pair<String, String>, Double> PAIR_SCORE_MAP = new HashMap<Pair<String, String>, Double>();

	static String tempClusters = "/tempClusters.tsv";
	static String tempClusterScores = "/tempScores.tsv";
	/**
	 * cluster collection
	 */
	static Map<String, List<String>> CLUSTER = new HashMap<String, List<String>>();

	private static int cnt;

	static String directory = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String scoreFile = null;
		String semiOptimalClusterFile = null;
		String fullyOptimalClusterFile = null;

		if (args.length != 3) {
			logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.cluster.analysis.ClusterOptimizer CONFIG.cfg <semi-optimal cluster output> <score File>");
		} else {
			Constants.loadConfigParameters(new String[] { "", args[0] });
			semiOptimalClusterFile = args[1];
			directory = new File(Constants.OIE_DATA_PATH).getParent();
			scoreFile = args[2];

			fullyOptimalClusterFile = directory
					+ "/clusters/optimalCluster.WF." + Constants.WORKFLOW
					+ ".beta." + Constants.OPTI_BETA + ".inf."
					+ Constants.OPTI_INFLATION + ".out";

			cnt = 1;

			logger.info("loading pairwise scores..");
			try {
				ClusterAnalyzer.loadScores(scoreFile, "\t");

				// get the pairwise scores
				PAIR_SCORE_MAP = ClusterAnalyzer.SCORE_MAP;

				logger.info("Optimising..bitte warten !!");

				// read the semi optimal clustered file into memory
				readMarkovClusters(semiOptimalClusterFile,
						Constants.OPTI_INFLATION);

				BufferedWriter clusterWriter = new BufferedWriter(
						new FileWriter(fullyOptimalClusterFile));

				logger.info("writing optimal cluster at "
						+ fullyOptimalClusterFile);

				for (Entry<String, List<String>> e : CLUSTER.entrySet()) {

					for (String elements : e.getValue()) {
						clusterWriter.write(elements + "\t");
					}
					clusterWriter.write("\n");
				}
				clusterWriter.flush();
				clusterWriter.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * read the cluster file to check and recluster if needed
	 * 
	 * @param clusterOutput
	 * @param oPTI_INFLATION
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	private static void readMarkovClusters(String clusterOutput, int inflation)
			throws IOException {

		Scanner scan;
		scan = new Scanner(new File((clusterOutput)), "UTF-8");

		List<String> list = null;

		String sCurrentLine = null;
		String[] elem = null;

		while (scan.hasNextLine()) {
			list = new ArrayList<String>();
			sCurrentLine = scan.nextLine();
			elem = sCurrentLine.split("\t");

			for (String s : elem)
				list.add(s);

			// still coarse try clustering again
			if (list.size() > 20 && lastSize != 5) {
				lastSize++;
				reCluster(list, inflation);
			} else {
				lastSize = 0;
				CLUSTER.put("C" + cnt++, list);
			}
		}

	}

	/**
	 * re-cluster a given list of relations
	 * 
	 * @param list
	 * @param inflation
	 * @param cnt
	 * @throws IOException
	 */
	private static void reCluster(List<String> list, int inflation)
			throws IOException {
		double val = 0;
		Pair<String, String> pair = null;

		String tempScores = directory + tempClusterScores;
		String tempOutput = directory + tempClusters;

		@SuppressWarnings("resource")
		BufferedWriter scoreWrtr = new BufferedWriter(
				new FileWriter(tempScores));

		for (int i = 0; i < list.size(); i++) {
			for (int j = i + 1; j < list.size(); j++) {
				pair = new ImmutablePair<String, String>(list.get(i).trim(),
						list.get(j).trim());

				try {
					val = PAIR_SCORE_MAP.get(pair);
				} catch (Exception e) {
					try {
						pair = new ImmutablePair<String, String>(list.get(j)
								.trim(), list.get(i).trim());
						val = PAIR_SCORE_MAP.get(pair);
					} catch (Exception e1) {
						val = 0;
					}
				}
				if (val > 0.2)
					scoreWrtr.write(pair.getLeft() + "\t" + pair.getRight()
							+ "\t" + val + "\n");
			}
			scoreWrtr.flush();
		}

		// make mcl call to perform clustering
		systemRoutine(inflation, tempScores, tempOutput);

		// read the output to load in memory
		readMarkovClusters(directory + tempClusters, inflation);

	}

	/**
	 * system call to run mcl on the new pair file
	 * 
	 * @param inflation
	 * @param tempScores
	 */
	private static void systemRoutine(int inflation, String tempScores,
			String output) {
		Runtime r = Runtime.getRuntime();

		try {

			Process p = r.exec("/home/adutta/Work/mcl/mcl-14-137/bin/mcl "
					+ tempScores + " --abc -I " + inflation + " -o " + output);

			BufferedReader bufferedreader = new BufferedReader(
					new InputStreamReader(new BufferedInputStream(
							p.getInputStream())));

			try {
				if (p.waitFor() != 0)
					System.err.println("exit value = " + p.exitValue());

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				bufferedreader.close();
			}

		} catch (Exception e) {
			System.err.println(e.getMessage());
		}

	}
}
