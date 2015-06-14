package code.dws.evaluation;

import gnu.trove.impl.hash.THash;
import gnu.trove.map.hash.THashMap;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.dto.FactDao;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

/**
 * primary class for evaluating the results of IM for Reverb
 * 
 * (A) Loads a gold file. (B) Calls the refined fact data base. (C) calls to the
 * top-1 IM. (A) vs (C) is the baseline (A) vs (B) is the algo mode
 * 
 * @author adutta
 *
 */
public class ReverbIMEvaluation {

	private static final int TOPK = 1;

	// define Logger
	static Logger logger = Logger.getLogger(ReverbIMEvaluation.class.getName());

	static Map<String, Long> pMap = new HashMap<String, Long>();

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

	static long glblNum = 0;
	static long glblDenom = 0;

	static long goldIMSize = 0;
	static long algoIMSize = 0;
	static long correctIMSize = 0;

	public ReverbIMEvaluation() {
	}

	public static void main(String[] args) {

		if (args.length != 2)
			logger.error("usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.evaluation.Evaluation CONFIG.cfg <GOLD file path>");
		else {
			Constants.loadConfigParameters(new String[] { "", args[0] });

			List<String> props = new ArrayList<String>();
			props.add("is located in");
			props.add("is in");
			props.add("was born in");
			props.add("is a registered trademark of");
			props.add("located in");
			props.add("is a suburb of");
			props.add("is part of");
			props.add("is the capital of");
			props.add("stands for");
			props.add("is a city in");
			props.add("originated in");
			props.add("is the home of");

			for (String p : props) {
				// load the respective gold standard and methods in memory
				setup(args[1], p);
				System.out.println("\n\n");
			}
			// perform comparison
			// compare();
		}
	}

	/**
	 * setup the gold file by loading the file in memory
	 * 
	 * @param goldFile
	 * @param p
	 * @param args
	 * 
	 */
	public static void setup(String goldFile, String rel) {
		FactDao dbpFact = null;

		try {
			pMap.clear();
			goldMapIM.clear();

			// load the gold file in memory
			loadGoldFile(goldFile, rel);

			for (Map.Entry<String, Long> e : pMap.entrySet()) {
				logger.info(e.getKey() + "\t" + e.getValue() + "\n");
			}
			// extract out exactly these gold facts from the DB output
			// to check the precision, recall, F1
			// hence we will create another collection of FactDao => FactDao and
			// compare these two maps

			computeBL();
			 computeAlgo();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void computeAlgo() {

		String subAlgo = null;
		String objAlgo = null;
		String subGold = null;
		String objGold = null;
		String oieSubj = null;
		String oieObj = null;

		FactDao dbpFact = null;

		reset();
		// init DB
		DBWrapper.init(Constants.GET_REFINED_FACT);

		for (Map.Entry<FactDao, FactDao> entry : goldMapIM.entrySet()) {

			// check the goldFact OIE triple,

			oieSubj = entry.getKey().getSub();
			oieObj = entry.getKey().getObj();
			subGold = entry.getValue().getSub();

			// ask for the Algo value from the DB
			dbpFact = DBWrapper.getRefinedDBPFact(entry.getKey());

			if (oieSubj != null) {

				// something mapped actually
				if (dbpFact != null) {
					subAlgo = dbpFact.getSub();
					if (!subGold.equals("?"))
						algoIMSize++;
				} else
					subAlgo = "?";

				// logger.info(oieSubj + "\t" + subGold + "\t"
				// + Utilities.utf8ToCharacter(subBL));

				if (!subGold.equals("?")) {
					goldIMSize++;
					if (subGold.trim().equals(subAlgo))
						correctIMSize++;
				}
			}

			if (oieObj != null) {
				objGold = entry.getValue().getObj();

				// something mapped actually
				if (dbpFact != null) {
					objAlgo = dbpFact.getObj();
					if (!objGold.equals("?"))
						algoIMSize++;
				} else
					objAlgo = "?";

				// logger.info(oieObj + "\t" + objGold + "\t"
				// + Utilities.utf8ToCharacter(objBL));

				if (!objGold.equals("?")) {
					goldIMSize++;
					if (objGold.trim().equals(objAlgo.trim()))
						correctIMSize++;
				}
			}
		}

		logger.info("GOLD IM  Count = " + goldIMSize);
		logger.info("Algo Precision = " + (double) correctIMSize / algoIMSize);
		logger.info("Algo Recall = " + (double) correctIMSize / goldIMSize);

		DBWrapper.shutDown();
	}

	private static void computeBL() {

		List<String> subBLList = null;
		List<String> objBLList = null;
		String subBL = null;
		String objBL = null;
		String subGold = null;
		String objGold = null;
		String oieSubj = null;
		String oieObj = null;

		reset();

		// init DB
		DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		for (Map.Entry<FactDao, FactDao> entry : goldMapIM.entrySet()) {

			// check the goldFact OIE triple,

			oieSubj = entry.getKey().getSub();
			oieObj = entry.getKey().getObj();
			subGold = entry.getValue().getSub();

			if (oieSubj != null) {

				// ask for the BL value that is the top-1 value
				subBLList = DBWrapper.fetchTopKLinksWikiPrepProb(oieSubj, TOPK);

				// something mapped actually
				if (subBLList.size() > 0) {

					for (String candSub : subBLList) {
						subBL = Utilities
								.utf8ToCharacter(candSub.split("\t")[0]);
						algoIMSize++;
						if (subGold.trim().equals(subBL))
							correctIMSize++;
					}

				} else
					subBL = "?";

				// logger.info(oieSubj + "\t" + subGold + "\t"
				// + Utilities.utf8ToCharacter(subBL));

				if (!subGold.equals("?")) {
					goldIMSize++;

				}

			}
			if (oieObj != null) {

				objGold = entry.getValue().getObj();

				objBLList = DBWrapper.fetchTopKLinksWikiPrepProb(oieObj, 1);

				// something mapped actually
				if (objBLList.size() > 0) {
					for (String candObj : objBLList) {
						objBL = Utilities
								.utf8ToCharacter(candObj.split("\t")[0]);
						algoIMSize++;
						if (objGold.trim().equals(objBL))
							correctIMSize++;
					}
				
				} else
					objBL = "?";

				// logger.info(oieObj + "\t" + objGold + "\t"
				// + Utilities.utf8ToCharacter(objBL));

				if (!objGold.equals("?")) {
					goldIMSize++;
				}
			}
		}

//		logger.info("GOLD IM  Count = " + goldIMSize);
		logger.info("BL Precision = " + (double) correctIMSize / algoIMSize);
		logger.info("BL Recall = " + (double) correctIMSize / goldIMSize);

		DBWrapper.shutDown();
	}

	private static void reset() {
		goldIMSize = 0;
		algoIMSize = 0;
		correctIMSize = 0;
	}

	/**
	 * load the gold standard file
	 * 
	 * @param goldFile
	 * @param rel
	 * @throws Exception
	 */
	private static void loadGoldFile(String goldFile, String rel)
			throws Exception {

		String[] arr = null;

		FactDao oieFact = null;
		FactDao dbpFact = null;

		// load the NELL file in memory as a collection
		List<String> gold = FileUtils.readLines(new File(goldFile));
		for (String line : gold) {
			arr = line.split("\t");

			if (isValidLine(arr) && arr[1].equals(rel)) {

				oieFact = new FactDao(arr[0], arr[1], arr[2]);

				dbpFact = new FactDao(StringUtils.replace(arr[3],
						Constants.DBPEDIA_INSTANCE_NS, ""), arr[4],
						StringUtils.replace(arr[5],
								Constants.DBPEDIA_INSTANCE_NS, ""));

				goldMapIM.put(oieFact, dbpFact);

				// updateMap(arr[1]);
			}
		}

		pMap = Utilities.sortByValue(pMap);

		logger.info("Done for " + rel);
	}

	private static void updateMap(String rel) {

		long val = 0;
		if (pMap.containsKey(rel)) {
			val = pMap.get(rel) + 1;
		} else
			val = 1;

		pMap.put(rel, val);

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

		return false;
	}

}
