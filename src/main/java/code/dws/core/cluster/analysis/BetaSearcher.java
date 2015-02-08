/**
 * 
 */
package code.dws.core.cluster.analysis;

import gnu.trove.map.hash.THashMap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

/**
 * COmbines the two pairwise sim files to compute a combined score file
 * 
 * @author adutta
 *
 */
public class BetaSearcher {

	// define Logger
	public static Logger logger = Logger
			.getLogger(BetaSearcher.class.getName());

	private static THashMap<Pair<String, String>, Double> wnMap = null;
	private static THashMap<Pair<String, String>, Double> ovlpMap = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.cluster.analysis.BetaSearcher <WN pairwise score file> <Overlap pairwise score file>");
		} else {
			String wnFile = args[0];
			String ovlpFile = args[1];

			String directory = new File(wnFile).getParent();

			BufferedWriter writer = null;

			logger.info("Loading " + wnFile + " and " + ovlpFile + " in memory");
			wnMap = loadInMemory(wnFile, wnMap);
			ovlpMap = loadInMemory(ovlpFile, ovlpMap);

			logger.info("Size of WN file map = " + wnMap.size());
			logger.info("Size of Overlap file map = " + ovlpMap.size());

			// for beta ranging from 0 to 1,
			for (float beta = 0; beta <= 10; beta++) {

				try {
					writer = new BufferedWriter(new FileWriter(directory
							+ "/sim.combined.beta." + ((double) beta / 10)
							+ ".pairs.ALL.OIE.csv"));

					// combine the two files in the ratio of beta
					combine((double) beta / 10, writer);

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * combine the two files as follows [(1-beta) * wnscore + (beta) *
	 * ovlpscore]
	 * 
	 * @param beta
	 * @param writer
	 * @throws IOException
	 */
	private static void combine(double beta, BufferedWriter writer)
			throws IOException {
		Pair<String, String> key = null;
		double wnSCore = 0;
		double ovlpSCore = 0;
		double finalScore = 0;

		for (Entry<Pair<String, String>, Double> entry : wnMap.entrySet()) {
			key = entry.getKey();
			wnSCore = entry.getValue();
			if (ovlpMap.containsKey(key)) {
				ovlpSCore = ovlpMap.get(key);
			}

			finalScore = beta * ovlpSCore + (1 - beta) * wnSCore;
			writer.write(key.getLeft() + "\t" + key.getRight() + "\t"
					+ finalScore + "\n");
			writer.flush();
		}
	}

	/**
	 * load the pairwise scored file into memory
	 * 
	 * @param file
	 * @param map
	 * @return
	 */
	private static THashMap<Pair<String, String>, Double> loadInMemory(
			String file, THashMap<Pair<String, String>, Double> map) {
		String line = null;
		String[] arr = null;
		Pair<String, String> key = null;

		map = new THashMap<Pair<String, String>, Double>();
		try {
			@SuppressWarnings("resource")
			Scanner scan = new Scanner(new File(file));

			while (scan.hasNextLine()) {
				line = scan.nextLine();
				arr = line.split("\t");

				key = new ImmutablePair<String, String>(arr[0], arr[1]);

				map.put(key, Double.valueOf(arr[2]));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		return map;
	}
}
