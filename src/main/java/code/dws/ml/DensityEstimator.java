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
import java.util.ArrayList;
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

import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.QuerySolution;
import com.ibm.icu.math.BigDecimal;

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

	static Map<String, Map<Pair<String, String>, Pair<Long, Double>>> PAIR_DISTRIBUTION = new HashMap<String, Map<Pair<String, String>, Pair<Long, Double>>>();
	
	static Map<String, Map<Pair<String, String>, Double>> RANKED_PAIR_DISTRIBUTION = new HashMap<String, Map<Pair<String, String>, Double>>();

	static Map<String, KernelEstimator> ESTIMATORS = new HashMap<String, KernelEstimator>();

	static Map<Pair<String, String>, KernelEstimator> MAP = new HashMap<Pair<String, String>, KernelEstimator>();

	private static double weight = 0.001;

	static Map<String, List<String>> CACHE = new HashMap<String, List<String>>();

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length != 2) {
			logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.ml.DensityEstimator CONFIG.cfg");
		} else {
			Constants.loadConfigParameters(new String[] { "", args[0] });

			// read it in memory

			loadTheSidePropertyDistribution(args[1]);

			// check the distribution
			rankTheDistribution();

			logger.info("Generating esimators for each OIE realtion");
			// feed an estimator
			createEstimators();
			logger.info("Done generating esimators for each OIE realtion");

//			System.exit(1);

			try {
				logger.info("Feeding esimators for each OIE realtion");
				runEstimators();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * key here is to look through the in memory DS and apply simple scaling and
	 * re-ranking to figure out the top-k possible side properties.
	 */
	private static void rankTheDistribution() {

		String rel = null;

		long N = 0;
		long ruleCount = 0;
		double coeff = 0;
		double newScore = 0;

		Pair<String, String> pair;

		for (Map.Entry<String, Map<Pair<String, String>, Pair<Long, Double>>> entry : PAIR_DISTRIBUTION
				.entrySet()) {
			rel = entry.getKey();

			Map<Pair<String, String>, Double> m = new HashMap<Pair<String, String>, Double>();

			if (rel.indexOf("is the county seat of") != -1)
				System.out.println();
			N = entry.getValue().size();
			for (Map.Entry<Pair<String, String>, Pair<Long, Double>> en : entry
					.getValue().entrySet()) {
				pair = en.getKey();
				ruleCount = en.getValue().getLeft();
				coeff = en.getValue().getRight();
				newScore = ((double) ruleCount / N) * Math.abs(coeff);

				if (!Double.isNaN(newScore))
					m.put(pair, newScore);
			}
			m = Utilities.sortByValue(m, 1);
			RANKED_PAIR_DISTRIBUTION.put(rel, m);
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
		double temp = 0;
		Pair<String, String> pair = null;

		for (Entry<String, Map<Pair<String, String>, Double>> entry : RANKED_PAIR_DISTRIBUTION
				.entrySet()) {
			temp = 0;
			oieRel = entry.getKey();

			// if there isnt any estimator predefined, create that
			if (!ESTIMATORS.containsKey(oieRel)) {
				// look for
				for (Entry<Pair<String, String>, Double> e : entry.getValue()
						.entrySet()) {
					pair = e.getKey();
				}
				// select the best pair for this relation
				getDataForMaxConfidentPair(oieRel, pair);
			}
		}
	}

	/**
	 * create estimator for each realtion from actual data from DBpedia
	 * 
	 * @param oieRel
	 * @param pair
	 */
	private static void getDataForMaxConfidentPair(String oieRel,
			Pair<String, String> pair) {
		String latDom = pair.getLeft();
		String latRan = pair.getRight();

		String subVal = null;
		String objVal = null;

		String query = null;

		double diff = 0;

		List<QuerySolution> list = null;

		KernelEstimator estimator = new KernelEstimator(0.0001);

		if (!MAP.containsKey(pair)) {
			query = "select distinct ?s1 ?o1 where {?s ?p ?o. "
					+ "?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>. "
					+ "?s <http://dbpedia.org/ontology/" + latDom + "> ?s1. "
					+ "?o <http://dbpedia.org/ontology/" + latRan
					+ "> ?o1. } LIMIT 1000";

			list = SPARQLEndPointQueryAPI.queryDBPediaEndPoint(query);

			// Get the next result row
			for (QuerySolution querySol : list) {

				// QuerySolution querySol = results.next();
				subVal = querySol.get("s1").toString();
				objVal = querySol.get("o1").toString();

				diff = getDifference(subVal, objVal);

				if (diff != Double.MAX_VALUE)
					estimator.addValue(diff, weight);
			}

			// caching it
			if (list.size() > 0) {
				MAP.put(pair, estimator);
			}
		} else {
			// if pre-exists just retrieve it
			estimator = MAP.get(pair);
		}

		logger.info("Adding estimator for " + oieRel + "; Size = "
				+ ESTIMATORS.size());
		ESTIMATORS.put(oieRel, estimator);

	}

	private static double getDifference(String subVal, String objVal) {
		Date date1 = null;
		String year1 = null;
		Date date2 = null;
		String year2 = null;

		double arg1 = 0;
		double arg2 = 0;

		try {
			date1 = formatDate.parse(subVal);
			year1 = formateYear.format(date1);

			date2 = formatDate.parse(objVal);
			year2 = formateYear.format(date2);

			arg1 = (year1 != null) ? Double.valueOf(year1) : 0;
			arg2 = (year2 != null) ? Double.valueOf(year2) : 0;

		} catch (ParseException e) {

			try {
				arg1 = new BigDecimal(StringUtils.substringBefore(subVal, "^"))
						.doubleValue();
				arg2 = new BigDecimal(StringUtils.substringBefore(objVal, "^"))
						.doubleValue();
			} catch (Exception e1) {
				// all non-comparable cases will come here
			}
		}

		if (arg1 > 0 && arg2 > 0)
			return arg1 - arg2;
		else
			return Double.MAX_VALUE;

	}

	/**
	 * go through the data set and look into all possible candidate combinations
	 * with each probability
	 * 
	 * @throws IOException
	 */
	private static void runEstimators() throws IOException {

		String[] elem = null;
		String oieSub = null;
		String oieRel = null;
		String oieObj = null;

		List<String> oieTriples = null;

		List<String> subCands = new ArrayList<String>();
		List<String> objCands = new ArrayList<String>();

		String kbSub = null;
		String kbObj = null;

		Pair<String, String> maxPair = null;
		Pair<String, String> pair = null;
		double count = 0;

		double dataPointVal = Double.MAX_VALUE;
		int ctr = 0;
		double max = 0;

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

				ctr++;

				// process only the properties valid for the workflow
				if (ESTIMATORS.containsKey(oieRel)) {
					// retrieve the pairs of side properties

					// get the top-k concepts, confidence pairs
					// UTF-8 at this stage. Write out all pairs with
					// probabilities

					if (!CACHE.containsKey(oieSub)) {
						subCands = DBWrapper.fetchTopKLinksWikiPrepProb(
								Utilities.cleanse(oieSub).replaceAll("\\_+",
										" "), 5);
						CACHE.put(oieSub, subCands);
					} else
						subCands = CACHE.get(oieSub);

					if (!CACHE.containsKey(oieObj)) {
						objCands = DBWrapper.fetchTopKLinksWikiPrepProb(
								Utilities.cleanse(oieObj).replaceAll("\\_+",
										" "), 5);
						CACHE.put(oieObj, objCands);
					} else
						objCands = CACHE.get(oieObj);

					for (Entry<Pair<String, String>, Double> entry : RANKED_PAIR_DISTRIBUTION
							.get(oieRel).entrySet()) {
						maxPair = entry.getKey();
					}

					for (String s : subCands) {
						kbSub = s.split("\t")[0];

						for (String o : objCands) {
							kbObj = o.split("\t")[0];

							if (kbSub != null && kbObj != null) {
								dataPointVal = getNumericValue(
										maxPair.getLeft(), maxPair.getRight(),
										Utilities.utf8ToCharacter(kbSub),
										Utilities.utf8ToCharacter(kbObj));

								// if its a valid data point, get its
								// probability
								if (dataPointVal < Double.MAX_VALUE) {
									writer.write(oieSub
											+ "\t"
											+ oieRel
											+ "\t"
											+ oieObj
											+ "\t"
											+ Utilities.utf8ToCharacter(kbSub)
											+ "\t"
											+ Utilities.utf8ToCharacter(kbObj)
											+ "\t"
											+ Constants.formatter
													.format(ESTIMATORS
															.get(oieRel)
															.getProbability(
																	dataPointVal))
											+ "\n");

									writer.flush();
								}
							}
						}
					}
				}

				if (ctr % 1000 == 0)
					logger.info("Completed " + (double) ctr * 100
							/ oieTriples.size());

			}
		} finally {
			writer.close();
			DBWrapper.shutDown();
		}

	}

	/**
	 * read the generated side props file ans load in memory
	 * 
	 * @param arg
	 */
	private static void loadTheSidePropertyDistribution(String arg) {
		String[] elem = null;

		List<String> propRelations = null;

		String location = new File(Constants.OIE_DATA_PATH).getParent() + "/"
				+ arg;

		logger.info("Loading the side property distributions");
		try {
			propRelations = FileUtils.readLines(new File(location), "UTF-8");
			for (String line : propRelations) {
				elem = line.split("\t");

				loadTheDistributionInMemory(
						elem[0],
						new ImmutablePair<String, String>(StringUtils.replace(
								elem[1], Constants.DBPEDIA_CONCEPT_NS, ""),
								StringUtils.replace(elem[2],
										Constants.DBPEDIA_CONCEPT_NS, "")),
						elem[3]);
			}
		} catch (IOException e) {
			logger.error("Problem while reading input OIE data file");
		}
	}

	/**
	 * create a a data structure out of it
	 * 
	 * @param oieRel
	 * @param immutablePair
	 * @param pearsonCoeff
	 */
	private static void loadTheDistributionInMemory(String oieRel,
			ImmutablePair<String, String> immutablePair, String pearsonCoeff) {
		long val = 0;

		Map<Pair<String, String>, Pair<Long, Double>> map = null;
		Pair<Long, Double> localPair = null;

		if (!PAIR_DISTRIBUTION.containsKey(oieRel)) {
			map = new HashMap<Pair<String, String>, Pair<Long, Double>>();
		} else {
			map = PAIR_DISTRIBUTION.get(oieRel);
		}

		if (map.size() == 0)
			val = 1;
		else {
			if (map.containsKey(immutablePair)) {
				val = map.get(immutablePair).getLeft();
				val = val + 1;
			} else
				val = 1;
		}
		localPair = new ImmutablePair<Long, Double>(val,
				Double.valueOf(pearsonCoeff));

		// one particular propertry pair will always have the same pearson
		// co-efficient
		map.put(immutablePair, localPair);

		PAIR_DISTRIBUTION.put(oieRel, map);
	}

	/**
	 * @param kbSub
	 * @param sideProperty
	 * @return
	 * @throws ParseException
	 */
	private static double getNumericValue(String domSide, String ranSide,
			String kbSub, String kbObj) {

		String queryStr = "select ?val where{{select ?val where{<http://dbpedia.org/resource/"
				+ kbSub
				+ "> <http://dbpedia.org/ontology/"
				+ domSide
				+ "> ?val} limit 1} union {select ?val where{<http://dbpedia.org/resource/"
				+ kbObj
				+ "> <http://dbpedia.org/ontology/"
				+ ranSide
				+ "> ?val} limit 1}}";

		// logger.info(query);
		ParameterizedSparqlString query = new ParameterizedSparqlString(
				queryStr);
		query.setIri("?kbSub", "http://dbpedia.org/resource/" + kbSub);
		query.setIri("?kbObj", "http://dbpedia.org/resource/" + kbObj);

		List<QuerySolution> list = SPARQLEndPointQueryAPI
				.queryDBPediaEndPoint(query.toString());

		if (list != null && list.size() == 2)
			return getDifference(list.get(0).get("val").toString(), list.get(1)
					.get("val").toString());
		else
			return Double.MAX_VALUE;
	}

	// private static double getDateVal(QuerySolution querySol) {
	// String dates;
	// Date date;
	// String year = null;
	//
	// dates = querySol.get("val").toString();
	// dates = StringUtils.substringBefore(dates, "^");
	//
	// try {
	// date = formatDate.parse(dates);
	// year = formateYear.format(date);
	// } catch (ParseException e) {
	// try {
	// year = new BigDecimal(dates).doubleValue();
	// } catch (Exception e1) {
	// // all non-comparable cases will come here
	// }
	// }
	// return (year != null) ? Double.valueOf(year) : 0;
	// }

}
