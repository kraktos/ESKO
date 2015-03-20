/**
 * 
 */
package code.dws.ml;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weka.estimators.KernelEstimator;
import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

import com.hp.hpl.jena.query.QuerySolution;

/**
 * @author adutta
 *
 */
public class DensityEstimator {

	/**
	 * logger
	 */
	public final static Logger logger = LoggerFactory
			.getLogger(GenerateSideProperties.class);

	static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");

	static SimpleDateFormat formateYear = new SimpleDateFormat("yyyy");

	static Map<String, Map<Pair<String, String>, Long>> GLBL_COLL = new HashMap<String, Map<Pair<String, String>, Long>>();

	static Map<String, KernelEstimator> ESTIMATORS = new HashMap<String, KernelEstimator>();

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length != 1) {
			logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.ml.DensityEstimator CONFIG.cfg");
		} else {
			Constants.loadConfigParameters(new String[] { "", args[0] });

			// read it in memory
			loadTheSideProperties();

			// check the distribution
			// print();

			// feed an estimator
			createEstimators();

			try {
				feedValues();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static void test() {
		String prop = "is managing editor of";
		KernelEstimator kde = ESTIMATORS.get(prop);
		logger.info("Std Deviation = " + kde.getStdDev());
		logger.info("Probability = " + kde.getProbability(40));
		logger.info("Probability = " + kde.getProbability(50));
		logger.info("Probability = " + kde.getProbability(500));
	}

	/**
	 * create an estimator on the values. Look the values from DBpedia and
	 * populate. Then we know for sure the exact value ranges possible.
	 */
	private static void createEstimators() {
		String oieRel = null;
		long temp = 0L;
		Pair<String, String> pair = null;

		for (Entry<String, Map<Pair<String, String>, Long>> entry : GLBL_COLL
				.entrySet()) {
			temp = 0L;
			oieRel = entry.getKey();

			// if there isnt any estimator predefined, create that
			if (!ESTIMATORS.containsKey(oieRel)) {
				// look for
				for (Entry<Pair<String, String>, Long> e : entry.getValue()
						.entrySet()) {
					if (e.getValue().longValue() > temp) {
						temp = e.getValue();
						pair = e.getKey();
					}
				}
				// select the best pair for this realtion
				getDataForThisPair(oieRel, pair);
			}
		}
	}

	/**
	 * create estimator for each realtion from actual data from DBpedia
	 * 
	 * @param oieRel
	 * @param pair
	 */
	private static void getDataForThisPair(String oieRel,
			Pair<String, String> pair) {
		String latDom = pair.getLeft();
		String latRan = pair.getRight();

		String subVal = null;
		String objVal = null;

		Date date1 = null;
		String year1 = null;
		Date date2 = null;
		String year2 = null;

		double arg1 = 0;
		double arg2 = 0;

		KernelEstimator estimator = new KernelEstimator(0.0001);

		String query = "select distinct ?s1 ?o1 where {?s ?p ?o. "
				+ "?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>. "
				+ "?s <http://dbpedia.org/ontology/" + latDom + "> ?s1. "
				+ "?o <http://dbpedia.org/ontology/" + latRan
				+ "> ?o1. } LIMIT 1000";

		List<QuerySolution> list = SPARQLEndPointQueryAPI
				.queryDBPediaEndPoint(query);

		// Get the next result row
		for (QuerySolution querySol : list) {

			// QuerySolution querySol = results.next();
			subVal = querySol.get("s1").toString();
			objVal = querySol.get("o1").toString();

			try {
				date1 = formatDate.parse(subVal);
				year1 = formateYear.format(date1);

				date2 = formatDate.parse(objVal);
				year2 = formateYear.format(date2);

			} catch (ParseException e) {
			}

			arg1 = (year1 != null) ? Double.valueOf(year1) : 0;
			arg2 = (year2 != null) ? Double.valueOf(year2) : 0;

			if (arg1 > 0 && arg2 > 0) {
				estimator.addValue(Math.abs(arg1 - arg2), 1);
			}
		}

		logger.info("Adding estimator for " + oieRel + "; Size = "
				+ ESTIMATORS.size());
		ESTIMATORS.put(oieRel, estimator);

	}

	/**
	 * go through the data set and look into all possible candidate combinations
	 * with each probability
	 * 
	 * @throws IOException
	 */
	private static void feedValues() throws IOException {

		String[] elem = null;
		String oieSub = null;
		String oieRel = null;
		String oieObj = null;

		List<String> oieTriples = null;

		List<String> subCands;
		List<String> objCands;

		String kbSub = null;
		String kbObj = null;

		double dataPointVal = 0;

		// load the data into memory
		try {
			oieTriples = FileUtils.readLines(new File(Constants.OIE_DATA_PATH),
					"UTF-8");

		} catch (IOException e) {
			logger.error("Problem while reaing input OIE data file");
		}

		// init DB
		DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				Constants.OIE_DATA_PATH).getParent()
				+ "/REVERB_MAPPING_PROB.tsv"));

		try {
			for (String line : oieTriples) {
				elem = line.split(Constants.OIE_DATA_SEPERARTOR);
				oieSub = elem[0];
				oieRel = elem[1];
				oieObj = elem[2];

				// process only the properties valid for the workflow
				if (ESTIMATORS.containsKey(oieRel)) {
					// retrieve the pairs of side properties

					// get the top-k concepts, confidence pairs
					// UTF-8 at this stage. Write out all pairs with
					// probabilities
					subCands = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
							.cleanse(oieSub).replaceAll("\\_+", " "), 5);
					objCands = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
							.cleanse(oieObj).replaceAll("\\_+", " "), 5);

					for (String s : subCands) {
						kbSub = s.split("\t")[0];
						for (String o : objCands) {
							kbObj = o.split("\t")[0];

							if (kbSub != null && kbObj != null) {
								dataPointVal = queryAllPairs(oieRel, kbSub,
										kbObj);
								// if its a valid data point, get its
								// probability
								if (dataPointVal != 0) {
									writer.write(oieSub
											+ "\t"
											+ oieRel
											+ "\t"
											+ oieObj
											+ "\t"
											+ kbSub
											+ "\t"
											+ kbObj
											+ "\t"
											+ Constants.formatter
													.format(ESTIMATORS
															.get(oieRel)
															.getProbability(
																	dataPointVal))
											+ "\n");
								}
							}
						}
					}
					writer.flush();
				}
			}
		} finally {
			writer.close();
			DBWrapper.shutDown();
		}

	}

	private static double queryAllPairs(String oieRel, String kbSub,
			String kbObj) {
		double subVal = 0;
		double objVal = 0;
		long temp = 0;
		Pair<String, String> pair = null;

		KernelEstimator estimator = null;

		// just iterate all the pairs and return the data value (difference in
		// lateral property values)
		for (Entry<Pair<String, String>, Long> entry : GLBL_COLL.get(oieRel)
				.entrySet()) {
			pair = entry.getKey();

			subVal = getNumericValue(pair.getLeft(), kbSub);
			objVal = getNumericValue(pair.getRight(), kbObj);

			// logger.info(subVal + "\t" + objVal + "\t" + oieRel);
			if (subVal > 0 && objVal > 0) {
				// logger.info(pair + "\t" + entry.getValue() + "\t" + oieRel);
				return Math.abs(subVal - objVal);
				// ESTIMATORS.put(oieRel, estimator);
				// break;
			}
		}
		return 0;
	}

	private static void print() {
		for (Entry<String, Map<Pair<String, String>, Long>> e : GLBL_COLL
				.entrySet()) {
			logger.info(e.getKey() + " ===> ");
			for (Entry<Pair<String, String>, Long> e2 : e.getValue().entrySet()) {
				logger.info(e2.getKey() + "\t" + e2.getValue());
			}
		}

	}

	/**
	 * read the generated side props file ans load in memory
	 */
	private static void loadTheSideProperties() {
		String[] elem = null;

		List<String> propRelations = null;

		String location = new File(Constants.OIE_DATA_PATH).getParent()
				+ GenerateSideProperties.FILE_FOR_SIDE_PROPS_DISTRIBUTION;

		try {
			propRelations = FileUtils.readLines(new File(location), "UTF-8");
			for (String line : propRelations) {
				elem = line.split("\t");

				writeOut(
						elem[0],
						new ImmutablePair<String, String>(StringUtils.replace(
								elem[1], Constants.DBPEDIA_CONCEPT_NS, ""),
								StringUtils.replace(elem[2],
										Constants.DBPEDIA_CONCEPT_NS, "")));
			}

		} catch (IOException e) {
			logger.error("Problem while reaing input OIE data file");
		}
	}

	/**
	 * create a a data structure out of it
	 * 
	 * @param oieRel
	 * @param immutablePair
	 */
	private static void writeOut(String oieRel,
			ImmutablePair<String, String> immutablePair) {
		long val = 0;

		Map<Pair<String, String>, Long> map = null;
		if (!GLBL_COLL.containsKey(oieRel)) {
			map = new HashMap<Pair<String, String>, Long>();
		} else {
			map = GLBL_COLL.get(oieRel);
		}

		if (map.containsKey(immutablePair))
			val = map.get(immutablePair);

		val = val + 1;
		map.put(immutablePair, Long.valueOf(val));
		GLBL_COLL.put(oieRel, map);
	}

	/**
	 * @param kbSub
	 * @param sideProperty
	 * @return
	 * @throws ParseException
	 */
	private static double getNumericValue(String sideProperty, String kbInst) {
		String dates = null;
		Date date = null;
		String year = null;

		List<QuerySolution> list = SPARQLEndPointQueryAPI
				.queryDBPediaEndPoint("select ?val where {<http://dbpedia.org/resource/"
						+ kbInst
						+ "> <http://dbpedia.org/ontology/"
						+ sideProperty + "> ?val}");
		if (list.size() == 0)
			list = SPARQLEndPointQueryAPI
					.queryDBPediaEndPoint("select ?val where {<http://dbpedia.org/resource/"
							+ kbInst
							+ "> <http://dbpedia.org/property/"
							+ sideProperty + "> ?val}");

		for (QuerySolution querySol : list) {
			// Get the next result row
			// QuerySolution querySol = results.next();
			dates = querySol.get("val").toString();
			dates = StringUtils.substringBefore(dates, "^");

			try {
				date = formatDate.parse(dates);
				year = formateYear.format(date);
			} catch (ParseException e) {
			}
		}
		return (year != null) ? Double.valueOf(year) : 0;
	}
}
