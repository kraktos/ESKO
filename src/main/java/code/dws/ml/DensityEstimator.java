/**
 * 
 */
package code.dws.ml;

import java.io.File;
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

import com.hp.hpl.jena.query.QuerySolution;

import weka.estimators.KernelEstimator;
import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

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
		}
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

			// look for
			for (Entry<Pair<String, String>, Long> e : entry.getValue()
					.entrySet()) {
				if (e.getValue().longValue() > temp) {
					temp = e.getValue();
					pair = e.getKey();
				}
			}

			getDataForThisPair(pair);
		}
	}

	private static void getDataForThisPair(Pair<String, String> pair) {
		String latDom = pair.getLeft();
		String latRan = pair.getRight();

		String query = "";
		logger.info(latDom + "\t" + latRan);
		
	}

	/**
	 * create a map of relation and individual estimators
	 */
	private static void feedValues() {

		String[] elem = null;
		String oieSub = null;
		String oieRel = null;
		String oieObj = null;

		List<String> oieTriples = null;

		List<String> subCands;
		List<String> objCands;

		String kbSub = null;
		String kbObj = null;

		String relation = null;
		KernelEstimator kEst = null;

		// load the data into memory
		try {
			oieTriples = FileUtils.readLines(new File(Constants.OIE_DATA_PATH),
					"UTF-8");

		} catch (IOException e) {
			logger.error("Problem while reaing input OIE data file");
		}

		// init DB
		DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		try {
			for (String line : oieTriples) {
				elem = line.split(Constants.OIE_DATA_SEPERARTOR);
				oieSub = elem[0];
				oieRel = elem[1];
				oieObj = elem[2];

				// process only the properties valid for the workflow
				if (GLBL_COLL.containsKey(oieRel)) {
					// retrieve the pairs of side properties

					// get the top-k concepts, confidence pairs
					// UTF-8 at this stage
					subCands = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
							.cleanse(oieSub).replaceAll("\\_+", " "), 1);
					objCands = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
							.cleanse(oieObj).replaceAll("\\_+", " "), 1);

					if (subCands != null && subCands.size() > 0)
						kbSub = subCands.get(0).split("\t")[0];
					if (objCands != null && objCands.size() > 0)
						kbObj = objCands.get(0).split("\t")[0];

					if (kbSub != null && kbObj != null) {
						queryAllValuePairs(oieRel, kbSub, kbObj);
					}
				}
			}
		} finally {
			DBWrapper.shutDown();
		}

	}

	private static void queryAllValuePairs(String oieRel, String kbSub,
			String kbObj) {
		double subVal = 0;
		double objVal = 0;
		long temp = 0;
		Pair<String, String> pair = null;

		KernelEstimator estimator = null;

		// if there is already an estimator predefined, retrieve that
		if (!ESTIMATORS.containsKey(oieRel)) {
			estimator = new KernelEstimator(0.0001);
		} else {
			estimator = ESTIMATORS.get(oieRel);
		}

		for (Entry<Pair<String, String>, Long> entry : GLBL_COLL.get(oieRel)
				.entrySet()) {
			pair = entry.getKey();

			subVal = getNumericValue(pair.getLeft(), kbSub);
			objVal = getNumericValue(pair.getRight(), kbObj);
			// logger.info(subVal + "\t" + objVal + "\t" + oieRel);
			if (subVal > 0 && objVal > 0) {
				logger.info(pair + "\t" + entry.getValue() + "\t" + oieRel);
				estimator.addValue(Math.abs(subVal - objVal), 1);
				ESTIMATORS.put(oieRel, estimator);
				break;
			}
		}
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
