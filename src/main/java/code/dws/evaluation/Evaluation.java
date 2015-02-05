package code.dws.evaluation;

import gnu.trove.map.hash.THashMap;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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
	 * gold standard file collection
	 */
	static THashMap<FactDao, FactDao> goldMap = new THashMap<FactDao, FactDao>();

	static THashMap<String, List<Pair<String, String>>> mapRltnCandidates = new THashMap<String, List<Pair<String, String>>>();

	/**
	 * method output collection
	 */
	static THashMap<FactDao, FactDao> algoMap = new THashMap<FactDao, FactDao>();

	public Evaluation() {
	}

	public static void main(String[] args) {

		if (args.length != 1)
			logger.error("usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.evaluation.Evaluation <GOLD file path>");
		else {
			// load the respective gold standard and methods in memory
			setup(args[0]);

			// perform comparison
			compare();
		}
	}

	/**
	 * setup the gold file by loading the file in memory
	 * 
	 * @param goldFile
	 * 
	 */
	public static void setup(String goldFile) {
		FactDao dbpFact = null;
		try {
			// init DB
			DBWrapper.init(Constants.GET_REFINED_FACT);

			// extract out exactly these gold facts from the algorithm output
			// to check the precision, recall, F1
			// hence we will create another collection of FactDao => FactDao and
			// compare these two maps

			for (Map.Entry<FactDao, FactDao> entry : loadGoldFile(goldFile)
					.entrySet()) {
				dbpFact = DBWrapper.getRefinedDBPFact(entry.getKey());

				if (dbpFact != null) {
					// logger.info(entry.getKey());
					// logger.info("GOLD ==>" + annotatedGoldFact);
					// logger.info("ALGO ==>" + dbpFact);

					// take the instances in Gold standard which have a
					// corresponding refinement done.
					algoMap.put(entry.getKey(), dbpFact);
				}
			}

			logger.info("GS Size = " + goldMap.size());
			logger.info("Algo Size = " + algoMap.size());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBWrapper.shutDown();
		}
	}

	/**
	 * load the gold standard file
	 * 
	 * @param goldFile
	 * @return
	 * @throws Exception
	 */
	private static THashMap<FactDao, FactDao> loadGoldFile(String goldFile)
			throws Exception {

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
					mapRltnCandidates.put(arr[1], rltnCandidates);

				goldMap.put(oieFact, dbpFact);
			}
		}
		return goldMap;
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

		double prec = computeScore("P");
		double recall = computeScore("R");

		logger.info("Precision = " + prec);
		logger.info("Recall = " + recall);
		logger.info("F1 = " + (double) 2 * recall * prec / (recall + prec));
	}

	/**
	 * Computes the precision, recall
	 * @param string
	 * @return
	 * 
	 */
	public static double computeScore(String identifier) {
		long numer = 0;
		long denom = 0;

		FactDao algoFact = null;
		FactDao goldFact = null;

		if (identifier.equals("P")) {
			// FOR PRECISION
			for (Map.Entry<FactDao, FactDao> entry : algoMap.entrySet()) {
				algoFact = entry.getValue();
				goldFact = goldMap.get(entry.getKey());

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
			for (Map.Entry<FactDao, FactDao> entry : goldMap.entrySet()) {
				goldFact = entry.getValue();

				// subjects
				// two things can happen, gold says not '?' or '?'
				if (!goldFact.getSub().trim().equals("?")) {
					algoFact = algoMap.get(entry.getKey());
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
					algoFact = algoMap.get(entry.getKey());
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

		return (double) numer / denom;
	}
}
