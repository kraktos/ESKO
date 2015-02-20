package code.dws.evaluation;

import gnu.trove.map.hash.THashMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.collections.ListUtils;
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
	static THashMap<FactDao, FactDao> algoMapIM = new THashMap<FactDao, FactDao>();
	static THashMap<FactDao, FactDao> prunedAlgoMapIM = new THashMap<FactDao, FactDao>();

	/**
	 * gold output collection for properties
	 */
	static THashMap<String, List<Pair<String, String>>> goldMapPM = new THashMap<String, List<Pair<String, String>>>();

	/**
	 * algo output collection for properties
	 */
	static THashMap<String, List<Pair<String, String>>> algoMapPM = new THashMap<String, List<Pair<String, String>>>();
	static THashMap<String, List<Pair<String, String>>> prunedAlgoMapPM = new THashMap<String, List<Pair<String, String>>>();

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
				logger.debug(entry.getKey());

				// try finding this oie triple from the algo map stored in
				// collection
				dbpFact = algoMapIM.get(entry.getKey());

				if (dbpFact != null) {
					// take the instances in Gold standard which have a
					// corresponding refinement done.
					prunedAlgoMapIM.put(entry.getKey(), dbpFact);
				}

				// also prune for the properties, present in the GOLD file.
				if (algoMapPM.contains(entry.getKey().getRelation()))
					prunedAlgoMapPM.put(entry.getKey().getRelation(),
							algoMapPM.get(entry.getKey().getRelation()));
			}

			logger.info("Algo triples Count = " + prunedAlgoMapIM.size());
			logger.info("Algo relations Count = " + prunedAlgoMapPM.size());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			algoMapIM.clear();
			algoMapIM = null;
			algoMapPM.clear();
			algoMapPM = null;
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
		Pair<String, String> pair = null;
		List<Pair<String, String>> valuePairs = null;

		try {
			scan = new Scanner(new File(algoFile), "UTF-8");
			// iterate the file
			while (scan.hasNextLine()) {
				line = scan.nextLine();
				arr = line.split("\t");

				oieFact = new FactDao(arr[0], arr[1], arr[2]);

				dbpFact = new FactDao(StringUtils.replace(arr[3],
						Constants.DBPEDIA_INSTANCE_NS, ""), "",
						StringUtils.replace(arr[5],
								Constants.DBPEDIA_INSTANCE_NS, ""));

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
				pair = new ImmutablePair<String, String>(StringUtils.replace(
						arr[4], Constants.DBPEDIA_CONCEPT_NS, ""), "");

				if (!valuePairs.contains(pair))
					valuePairs.add(pair);

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
				else {
					logger.info("Thullu with " + arr[1]);
				}

				goldMapIM.put(oieFact, dbpFact);
			}
		}

		logger.info("Properties entered into GOLD PM = " + goldMapPM.size());
	}

	/**
	 * checks everu GOld standard line, and determines which should be in or out
	 * 
	 * @param arr
	 * @return
	 */
	private static boolean isValidLine(String[] arr) {
		if (arr.length == 6) {
			if (arr[0].length() > 0 && arr[1].length() > 0
					&& arr[2].length() > 0 && arr[3].length() > 0
					&& arr[5].length() > 0)
				return true;
		}

		// unblock to check which annotated gold lines are missed
		// else {
		// if (arr.length > 3 && arr[0].length() > 0 && arr[1].length() > 0
		// && arr[2].length() > 0)
		// logger.info(arr[0] + "\t" + arr[1] + "\t" + arr[2]);
		// }
		return false;
	}

	/**
	 * output the metrics values
	 */
	private static void compare() {

		double imPrec = computeIMScore("P");
		double imRecall = computeIMScore("R");

		logger.info("IM Precision = " + 100 * imPrec);
		logger.info("IM Recall = " + 100 * imRecall);
		if (imPrec == 0 || imRecall == 0)
			logger.info("PM F1 = " + 0.0);
		else
			logger.info("IM F1 = " + (double) 200 * imRecall * imPrec
					/ (imRecall + imPrec));

		double pmPrec = computePMScore("P");
		double pmRecall = computePMScore("R");

		logger.info("PM Precision = " + 100 * pmPrec);
		logger.info("PM Recall = " + 100 * pmRecall);
		if (pmRecall == 0 || pmPrec == 0)
			logger.info("PM F1 = " + 0.0);
		else
			logger.info("PM F1 = " + (double) 200 * pmRecall * pmPrec
					/ (pmRecall + pmPrec));

	}

	/*
	 * takes two lists, gold and algo and tries to check if it is a match or not
	 */
	private static boolean match(List<Pair<String, String>> algoPMCands,
			List<Pair<String, String>> goldPMCands) {

		List<String> algoVals = new ArrayList<String>();
		List<String> goldVals = new ArrayList<String>();

		for (Pair<String, String> algCands : algoPMCands) {
			algoVals.add(algCands.getLeft());
		}

		for (Pair<String, String> goldCands : goldPMCands) {
			goldVals.add(goldCands.getLeft());
		}

		@SuppressWarnings("unchecked")
		List<String> intersect = ListUtils.intersection(algoVals, goldVals);
		if (intersect.size() > 0)
			return true;
		else {
			logger.info("Algo = " + algoVals);
			logger.info("Gold = " + goldVals);
			return false;
		}

	}

	/**
	 * computes the precision, recall for property matching (PM)
	 * 
	 * @param identifier
	 * @return
	 */
	private static double computePMScore(String identifier) {
		long numer = 0;
		long denom = 0;
		String oieRelation = null;
		List<Pair<String, String>> algoPMCands = null;
		List<Pair<String, String>> goldPMCands = null;

		if (identifier.equals("P")) {
			for (Entry<String, List<Pair<String, String>>> entry : prunedAlgoMapPM
					.entrySet()) {

				oieRelation = entry.getKey();
				algoPMCands = entry.getValue();
				goldPMCands = goldMapPM.get(oieRelation);

				if (match(algoPMCands, goldPMCands)) {
					numer++;
				} else {
					logger.info("Garbr for = " + oieRelation);
				}

				denom++;
			}
		}

		if (identifier.equals("R")) {
			for (Entry<String, List<Pair<String, String>>> entry : goldMapPM
					.entrySet()) {

				oieRelation = entry.getKey();
				goldPMCands = entry.getValue();
				algoPMCands = prunedAlgoMapPM.get(oieRelation);

				if (algoPMCands != null) {
					if (match(algoPMCands, goldPMCands)) {
						numer++;
					}
				}
				denom++;
			}
		}

		if (denom == 0)
			return 0;

		// System.out.printf("%d %d \n", numer, denom);
		return (double) numer / denom;

	}

	/**
	 * Computes the precision, recall for instance matching (IM)
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

		// FOR PRECISION
		if (identifier.equals("P")) {
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
						// denom++;
					} else {
						if (!goldFact.getSub().equals("?")) {

						}
					}

					denom++;
				}

				// objects
				// two things can happen, algo says not '?' or '?'
				if (!algoFact.getObj().equals("?")) {
					// now it can be right or wrong once it is not '?'
					if (algoFact.getObj().equals(goldFact.getObj())) {
						// a correct match, increment
						numer++;
						// denom++;
					} else {
						if (!goldFact.getObj().equals("?")) {
							// denom++;
						}
					}

					denom++;
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
