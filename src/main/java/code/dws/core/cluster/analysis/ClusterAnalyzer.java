/**
 * 
 */
package code.dws.core.cluster.analysis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.utils.Constants;

/**
 * This class looks into the generated clusters, output from the mcl routine and
 * tries to find an intrinsic score of cluster quality
 * 
 * @author adutta
 *
 */
public class ClusterAnalyzer {

	// define Logger
	public static Logger logger = Logger.getLogger(ClusterAnalyzer.class
			.getName());

	private static Map<String, List<String>> CLUSTER = new HashMap<String, List<String>>();

	public static Map<Pair<String, String>, Double> SCORE_MAP = new HashMap<Pair<String, String>, Double>();

	private static double BEST_SCORE = Double.MAX_VALUE;

	private static Map<String, List<String>> OIE_DBP_PROP_MAP = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String directory = null;
		String clusterOutputs = null;
		String pairScoreFile = null;

		if (args.length != 3) {
			logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.cluster.analysis.ClusterAnalyzer CONFIG.cfg <where to look for clusters> <pairwise score file>");
		} else {
			Constants.loadConfigParameters(new String[] { "", args[0] });

			directory = new File(Constants.OIE_DATA_PATH).getParent();
			clusterOutputs = args[1];
			pairScoreFile = args[2];

			try {
				scanAndWriteClusterScores(directory, clusterOutputs,
						pairScoreFile);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * return mappings of OIE relation to DBP relations
	 * 
	 * @return
	 */
	public static Map<String, List<String>> getOIE2KBRelationMappings() {
		return OIE_DBP_PROP_MAP;
	}

	/**
	 * retrieve the cluster for the given path
	 * 
	 * @param optimalClusterPAth
	 * @return
	 */
	public static Map<String, List<String>> getOptimalCluster(
			String optimalClusterPAth) {
		try {
			readMarkovClusters(optimalClusterPAth);
			return CLUSTER;
		} catch (IOException e) {
			logger.error("Error in getClusterScore()");
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * retrieve the cluster for the given path
	 * 
	 * @param optimalClusterPath
	 * @return
	 */
	public static Map<String, List<String>> getKBRelMappings(
			String optimalClusterPath) {
		try {
			readMarkovClusters(optimalClusterPath);
			return OIE_DBP_PROP_MAP;
		} catch (IOException e) {
			logger.error("Error in getClusterScore()");
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * look into the given directory and find the cluster score for each cluster
	 * configuration
	 * 
	 * @param directory
	 * @param clusterOutputs
	 * @param pairScoreFile
	 * @throws IOException
	 */
	private static void scanAndWriteClusterScores(String directory,
			String clusterOutputs, String pairScoreFile) throws IOException {
		double mclIndex = 0;
		int inf = 0;
		double beta = 0.5;
		Matcher matcher = null;

		Path clustersOutputPath = Paths.get(clusterOutputs + "/");

		BufferedWriter writer = new BufferedWriter(new FileWriter(
				clusterOutputs + "/ClusterScoresTable.tsv"));

		writer.write("BETA\tITERATION\tCLUSTER_SIZE\tMCL_SCORE\n");

		// patterns for extracting the beta and inflation values from the file
		// name
		Pattern patternBeta = Pattern.compile("beta.(.+?).inf");
		Pattern patternInf = Pattern.compile("inf.(.+?).out");

		// VISIT THE FOLDER LOCATION AND ITERATE THROUGH ALL SUB FOLDERS FOR
		// THE EXACT OUTPUT FILE
		final List<Path> files = new ArrayList<Path>();
		FileVisitor<Path> fv = new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file,
					BasicFileAttributes attrs) throws IOException {
				if (file.toString().endsWith(".out"))
					files.add(file);
				return FileVisitResult.CONTINUE;
			}
		};

		// load the pairwise scores file for the given beta
		loadScores(pairScoreFile, "\t");

		try {
			// gets the only relevant output files
			Files.walkFileTree(clustersOutputPath, fv);

			// iterate the files
			for (Path path : files) {
				CLUSTER = new HashMap<String, List<String>>();
				logger.info("Currently in location " + path + " .... ");

				matcher = patternBeta.matcher(path.toString());
				while (matcher.find()) {
					beta = Double.valueOf(matcher.group(1));
					beta = (double) beta / 10;
				}
				matcher = patternInf.matcher(path.toString());
				while (matcher.find()) {
					inf = Integer.valueOf(matcher.group(1));
				}

				// extracted the beta and inflation values
				logger.debug(beta + " \t " + inf);

				if (0 < beta && beta < 1) {

					// // load the pairwise scores file for the given beta
					// loadScores(directory + "/sim.combined.beta." + beta
					// + ".pairs.ALL.OIE.csv", "\t");

					// read the particular cluster file
					readMarkovClusters(path.toString());

					// compute its cluster score
					mclIndex = computeClusterIndex(CLUSTER);

					// create a map of cluster key and its highest isolation
					// value
					logger.info("MCL Index Score = " + mclIndex);
					writer.write(beta + "\t" + inf + "\t" + CLUSTER.size()
							+ "\t" + mclIndex + "\n");

					writer.flush();

					// scheme for the best score
					if (mclIndex < BEST_SCORE) {
						BEST_SCORE = mclIndex;
						Constants.OPTI_INFLATION = inf;
						Constants.OPTI_BETA = beta;
					}
				}
			}

			logger.info("Best at " + Constants.OPTI_INFLATION + "\t"
					+ Constants.OPTI_BETA);
		} catch (IOException e) {
			e.printStackTrace();
		}
		writer.close();
	}

	/**
	 * get a isolation score for the whole cluster set
	 * 
	 * @param mCl
	 * @param isoMcl
	 * @return
	 * @return
	 */
	private static double computeClusterIndex(Map<String, List<String>> mCl) {

		double minCompactness = 0;
		double tempIsolation = 0;
		double maxIsolation = 0;

		List<String> arg1 = null;
		List<String> arg2 = null;

		double clusterGoodness = 0;

		logger.info("Computing score for this cluster..");
		for (Entry<String, List<String>> e1 : mCl.entrySet()) {

			maxIsolation = 0;

			for (Entry<String, List<String>> e2 : mCl.entrySet()) {
				if (e2.getKey().hashCode() != e1.getKey().hashCode()) {

					arg1 = e1.getValue();
					arg2 = e2.getValue();
					tempIsolation = intraClusterScore(arg1, arg2);

					// get the maximum score, i.e the strongest intra-cluster
					// pair..
					maxIsolation = (maxIsolation < tempIsolation) ? tempIsolation
							: maxIsolation;
				}
			}

			// perform its own compactness
			minCompactness = getInterClusterScore(e1.getValue());

			clusterGoodness = clusterGoodness + (double) minCompactness
					/ ((maxIsolation == 0) ? Math.pow(10, -1) : maxIsolation);

		}

		clusterGoodness = (clusterGoodness == 0) ? (Math.pow(10, -8) - clusterGoodness)
				: clusterGoodness;

		return (double) 1 / clusterGoodness;

	}

	/**
	 * get the pairwise similarity scores for each elements in a cluster
	 * 
	 * @param cluster
	 * @return
	 */
	private static double getInterClusterScore(List<String> cluster) {

		Pair<String, String> pair = null;

		double score = 1;
		double tempScore = 0;

		// System.out.println("CL size " + cluster.size());
		if (cluster.size() <= 1)
			return 0;

		for (int outer = 0; outer < cluster.size(); outer++) {
			for (int inner = outer + 1; inner < cluster.size(); inner++) {
				// create a pair
				pair = new ImmutablePair<String, String>(cluster.get(outer)
						.trim(), cluster.get(inner).trim());

				try {
					// retrieve the key from the collection
					tempScore = SCORE_MAP.get(pair);
				} catch (Exception e) {
					try {
						pair = new ImmutablePair<String, String>(cluster.get(
								inner).trim(), cluster.get(outer).trim());
						tempScore = SCORE_MAP.get(pair);

					} catch (Exception e1) {
						tempScore = 0;
					}
				}

				// for sum of all pairwise scores
				// score = score + tempScore;

				// for the minimum inter cluster score
				score = (score >= tempScore) ? tempScore : score;
			}
		}
		return score;
	}

	/**
	 * load the file with pairwise scores for a given beta
	 * 
	 * @param file
	 * @param delimit
	 * @throws FileNotFoundException
	 */
	@SuppressWarnings("resource")
	public static void loadScores(String file, String delimit)
			throws FileNotFoundException {

		String sCurrentLine;
		double score;

		Scanner scan;
		scan = new Scanner(new File(file), "UTF-8");

		Pair<String, String> pair = null;

		while (scan.hasNextLine()) {
			sCurrentLine = scan.nextLine();

			pair = new ImmutablePair<String, String>(
					sCurrentLine.split(delimit)[0].trim(),
					sCurrentLine.split(delimit)[1].trim());

			score = Double.valueOf(sCurrentLine.split(delimit)[2]);

			SCORE_MAP.put(pair, score);
		}

		logger.info("Loaded file " + file + " in memory");

	}

	/**
	 * take 2 lists and find the max pairwise similarity score. this essentially
	 * gives the isolation score of two clusters represented by two lists
	 * 
	 * @param arg1
	 * @param arg2
	 * @return
	 */
	private static double intraClusterScore(List<String> arg1, List<String> arg2) {
		// compare each elements from argList1 vs argList2

		Pair<String, String> pair = null;
		double tempScore = 0;
		double maxScore = 0;

		for (int list1Id = 0; list1Id < arg1.size(); list1Id++) {
			for (int list2Id = 0; list2Id < arg2.size(); list2Id++) {

				// create a pair
				pair = new ImmutablePair<String, String>(arg1.get(list1Id)
						.trim(), arg2.get(list2Id).trim());

				try {
					// retrieve the key from the collection
					tempScore = SCORE_MAP.get(pair);
				} catch (Exception e) {
					try {
						pair = new ImmutablePair<String, String>(arg2.get(
								list2Id).trim(), arg1.get(list1Id).trim());
						tempScore = SCORE_MAP.get(pair);

					} catch (Exception e1) {
						tempScore = 0;
					}
				}

				// System.out.println(" temp score = " + tempScore);
				maxScore = (tempScore > maxScore) ? tempScore : maxScore;
			}
		}

		// System.out.println("max = " + maxScore);
		return maxScore;
	}

	/**
	 * read the particular cluster file
	 * 
	 * @param output
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	private static void readMarkovClusters(String output) throws IOException {
		List<String> dbpProps = null;
		Scanner scan;
		scan = new Scanner(new File((output)), "UTF-8");
		int cnt = 1;

		List<String> list = null;
		String sCurrentLine = null;
		String[] elements = null;

		int cntr = 0;
		OIE_DBP_PROP_MAP = new HashMap<String, List<String>>();

		while (scan.hasNextLine()) {
			list = new ArrayList<String>();
			sCurrentLine = scan.nextLine();

			dbpProps = new ArrayList<String>();

			elements = sCurrentLine.split("\t");
			for (String element : elements) {
				if (element.indexOf(" ") == -1) {// presence of dbpedia property
					dbpProps.add(element);
				} else {
					list.add(element);
				}
			}

			if (dbpProps.size() > 0 && list.size() > 0) {
				cntr++;
				for (String elem : elements) {
					if (elem.indexOf(" ") != -1) {
						OIE_DBP_PROP_MAP.put(elem, dbpProps);
					}
				}
			}

			if (Constants.WORKFLOW == 3) {
				if (list.size() > 0 && dbpProps.size() > 0)
					CLUSTER.put("C" + cnt++, list);
				dbpProps = null;
			} else if (list.size() > 0)
				CLUSTER.put("C" + cnt++, list);
		}

		logger.info("Total Cluster = " + (cnt - 1));
		logger.info("Total Clusters  mapped = " + cntr);
	}

	public static String getOptimalClusterPath() {
		return new File(Constants.OIE_DATA_PATH).getParent()
				+ "/clusters/optimalCluster.WF." + Constants.WORKFLOW
				+ ".beta." + Constants.OPTI_BETA + ".inf."
				+ Constants.OPTI_INFLATION + ".out";
	}

}
