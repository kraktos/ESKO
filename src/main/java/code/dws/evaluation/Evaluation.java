package code.dws.evaluation;

import gnu.trove.map.hash.THashMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.dto.FactDao;
import code.dws.utils.Constants;

/**
 * primary class for evaluating the results
 * 
 * @author adutta
 *
 */
public class Evaluation {

	// define Logger
	static Logger logger = Logger.getLogger(Evaluation.class.getName());

	/**
	 * gold standard file collection for instances
	 */
	static THashMap<FactDao, FactDao> goldMapIM = new THashMap<FactDao, FactDao>();

	/**
	 * algo output collection for instances, full and pruned
	 */
	static THashMap<FactDao, FactDao> prunedAlgoMapIM = new THashMap<FactDao, FactDao>();
	static THashMap<FactDao, FactDao> algoMapIM = new THashMap<FactDao, FactDao>();

	/**
	 * gold output collection for properties
	 */
	static THashMap<String, List<Pair<String, String>>> goldMapPM = new THashMap<String, List<Pair<String, String>>>();

	/**
	 * algo output collection for properties
	 */
	static THashMap<String, List<Pair<String, String>>> algoMapPM = new THashMap<String, List<Pair<String, String>>>();

	static THashMap<String, List<Pair<String, String>>> mapRltnCandidates = new THashMap<String, List<Pair<String, String>>>();

	public Evaluation() {
	}

	public static void main(String[] args) {

		if (args.length != 2)
			logger.error("usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.evaluation.Evaluation <GOLD file path> <NEW triples file>");
		else {
			// load the respective gold standard and methods in memory
			setup(args[0], args[1]);

			// perform comparison
			compare();
		}
	}

	/**
	 * setup the gold file by loading the file in memory
	 * 
	 * @param goldFile
	 * @param args
	 * 
	 */
	public static void setup(String goldFile, String algoFile) {
		FactDao dbpFact = null;
		try {
			// init DB
			DBWrapper.init(Constants.GET_REFINED_FACT);

			// load the two files in memory
			loadGoldFile(goldFile);
			loadAlgoFile(algoFile);

			// extract out exactly these gold facts from the algorithm output
			// to check the precision, recall, F1
			// hence we will create another collection of FactDao => FactDao and
			// compare these two maps

			for (Map.Entry<FactDao, FactDao> entry : goldMapIM.entrySet()) {

				// check the goldFact OIE triple,
				logger.info(entry.getKey());

				// try finding this oie triple from the algo map stored in
				// collection
				dbpFact = algoMapIM.get(entry.getKey());

				if (dbpFact != null) {
					// take the instances in Gold standard which have a
					// corresponding refinement done.
					prunedAlgoMapIM.put(entry.getKey(), dbpFact);
				}
			}

			logger.info("GS Size = " + goldMapIM.size());
			logger.info("Algo Size = " + prunedAlgoMapIM.size());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			algoMapIM.clear();
			algoMapIM = null;
		}
	}

	/**
	 * reads the new triple generated file and loads the IMs in memory
	 * 
	 * @param algoFile
	 */
	private static void loadAlgoFile(String algoFile) {
		String line = null;
		Scanner scan = null;
		String[] arr = null;

		FactDao oieFact = null;
		FactDao dbpFact = null;

		List<Pair<String, String>> valuePairs = null;

		try {
			scan = new Scanner(new File(algoFile), "UTF-8");
			// iterate the file
			while (scan.hasNextLine()) {
				line = scan.nextLine();
				arr = line.split("\t");

				oieFact = new FactDao(arr[0], arr[1], arr[2]);
				dbpFact = new FactDao(arr[3], "", arr[5]);

				// load the instances
				algoMapIM.put(oieFact, dbpFact);

				// load the properties
				if (algoMapPM.containsKey(arr[1])) {
					valuePairs = algoMapPM.get(arr[1]);
				} else {
					valuePairs = new ArrayList<Pair<String, String>>();
				}
				// made a list of pairs to be in sync with the gold data
				// structure. left element is the algo property, right is just
				// blank
				valuePairs.add(new ImmutablePair<String, String>(arr[4], ""));
				algoMapPM.put(arr[1], valuePairs);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * load the gold standard file
	 * 
	 * @param goldFile
	 * @throws Exception
	 */
	private static void loadGoldFile(String goldFile) throws Exception {

		String[] arr = null;

		FactDao oieFact = null;
		FactDao dbpFact = null;

		List<Pair<String, String>> rltnCandidates = null;

		// load the NELL file in memory as a collection
		List<String> gold = FileUtils.readLines(new File(goldFile));
		for (String line : gold) {
			arr = line.split("\t");

			if (isValidLine(arr)) {

				oieFact = new FactDao(arr[0], arr[1], arr[2]);

				dbpFact = new FactDao(StringUtils.replace(arr[3],
						Constants.DBPEDIA_INSTANCE_NS, ""), arr[4],
						StringUtils.replace(arr[5],
								Constants.DBPEDIA_INSTANCE_NS, ""));

				// get the KB relations for this OIE relation
				rltnCandidates = DBWrapper
						.fetchKBRelationsForAnOIERelation(arr[1]);

				// store it in memory for access
				if (rltnCandidates.size() > 0)
					goldMapPM.put(arr[1], rltnCandidates);

				goldMapIM.put(oieFact, dbpFact);
			}
		}
	}

	private static boolean isValidLine(String[] arr) {
		if (arr.length == 6) {
			if (arr[0].length() > 0 && arr[1].length() > 0
					&& arr[2].length() > 0 && arr[3].length() > 0
					&& arr[5].length() > 0)
				return true;
		}
		return false;
	}

	/**
	 * output the metrics values
	 */
	private static void compare() {

		double prec = computePMScore("P");
		double recall = computePMScore("R");

		logger.info("Instance Precision = " + prec);
		logger.info("Instance Recall = " + recall);
		logger.info("Instance F1 = " + (double) 2 * recall * prec
				/ (recall + prec));

		double prec = computeIMScore("P");
		double recall = computeIMScore("R");

		logger.info("Instance Precision = " + prec);
		logger.info("Instance Recall = " + recall);
		logger.info("Instance F1 = " + (double) 2 * recall * prec
				/ (recall + prec));

	}

	private static double computePMScore(String identifier) {
		long numer = 0;
		long denom = 0;
		if (identifier.equals("P")) {
			
		}
		return 0;
	}

	/**
	 * Computes the precision, recall
	 * 
	 * @param string
	 * @return
	 * 
	 */
	public static double computeIMScore(String identifier) {
		long numer = 0;
		long denom = 0;

		FactDao algoFact = null;
		FactDao goldFact = null;

		if (identifier.equals("P")) {
			// FOR PRECISION
			for (Map.Entry<FactDao, FactDao> entry : prunedAlgoMapIM.entrySet()) {
				algoFact = entry.getValue();
				goldFact = goldMapIM.get(entry.getKey());

				// subjects
				// two things can happen, algo says not '?' or '?'
				if (!algoFact.getSub().equals("?")) {
					// now it can be right or wrong once it is not '?'
					if (algoFact.getSub().equals(goldFact.getSub())) {
						// a correct match, increment
						numer++;
						denom++;
					} else {
						if (!goldFact.getSub().equals("?")) {
							denom++;
						}
					}
				}

				// objects
				// two things can happen, algo says not '?' or '?'
				if (!algoFact.getObj().equals("?")) {
					// now it can be right or wrong once it is not '?'
					if (algoFact.getObj().equals(goldFact.getObj())) {
						// a correct match, increment
						numer++;
						denom++;
					} else {
						if (!goldFact.getObj().equals("?")) {
							denom++;
						}
					}
				}
			}
		}

		if (identifier.equals("R")) {
			for (Map.Entry<FactDao, FactDao> entry : goldMapIM.entrySet()) {
				goldFact = entry.getValue();

				// subjects
				// two things can happen, gold says not '?' or '?'
				if (!goldFact.getSub().trim().equals("?")) {
					algoFact = prunedAlgoMapIM.get(entry.getKey());
					if (algoFact != null) {
						// now it can be right or wrong once it is not '?'
						if (goldFact.getSub().equals(algoFact.getSub())) {
							// a correct match, increment
							numer++;
						}
					}
					denom++;

				} // do not bother when gold is '?'

				// objects
				// two things can happen, gold says not '?' or '?'
				if (!goldFact.getObj().trim().equals("?")) {
					algoFact = prunedAlgoMapIM.get(entry.getKey());
					if (algoFact != null) {
						// now it can be right or wrong once it is not '?'
						if (goldFact.getObj().equals(algoFact.getObj())) {
							// a correct match, increment
							numer++;
						}
					}
					denom++;
				} // do not bother when gold is '?'
			}
		}

		if (denom == 0)
			return 0;

		return (double) numer / denom;
	}
}
