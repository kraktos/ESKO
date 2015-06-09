/**
 * 
 */
package code.dws.ml;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.setup.Generator;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.QuerySolution;

/**
 * Class for estimating kernel densities. Used for detecting incorrect instance
 * matches
 * 
 * @author arnab
 */
public class GenerateSideProperties {

	public static final String SIDE_PROP_CORRELATION = "/REVERB_NUMERIC_SIDE_PROPS_CORRELATION.tsv";
	public static final String SIDE_PROP_COUNTS = "/REVERB_NUMERIC_SIDE_PROPS_COUNTS.tsv";

	/**
	 * logger
	 */
	public final static Logger logger = LoggerFactory
			.getLogger(GenerateSideProperties.class);

	static Map<String, Long> propMap = new HashMap<String, Long>();

	static Map<String, String> CACHE = new HashMap<String, String>();

	static Map<Pair<String, String>, Double> CACHE_PAIRS_PEARSON = new HashMap<Pair<String, String>, Double>();

	private static Map<String, Map<Pair<String, String>, Long>> COUNT_MAP = new HashMap<String, Map<Pair<String, String>, Long>>();

	/**
	 * @param args
	 * @throws ParseException
	 */
	public static void main(String[] args) throws ParseException {
		List<String> oieTriples = null;

		if (args.length != 1) {
			logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.ml.DensityEstimator CONFIG.cfg");
		} else {
			Constants.loadConfigParameters(new String[] { "", args[0] });

			// load the reverb properties
			try {
				oieTriples = FileUtils.readLines(new File(
						Constants.OIE_DATA_PATH), "UTF-8");

			} catch (IOException e) {
				logger.error("Problem while reaing input OIE data file");
			}

			if (oieTriples != null) {
				logger.info("Loaded " + oieTriples.size() + " triples");

				parseTheTriples(oieTriples);
			}
		}
	}

	/**
	 * build a distribution from the individual mappings
	 * 
	 * @param oieTriples
	 * @throws ParseException
	 */
	private static void parseTheTriples(List<String> oieTriples)
			throws ParseException {

		int ctr = 0;

		String[] elem = null;
		String oieSub = null;
		String oieRel = null;
		String oieObj = null;

		String kbSub = null;
		String kbObj = null;

		List<String> subCands = null;
		List<String> objCands = null;
		List<String> domainSideProps = null;
		List<String> rangeSideProps = null;

		BufferedWriter writer = null;
		BufferedWriter writerCounts = null;

		// init DB
		DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		getValidProps();

		try {
			writer = new BufferedWriter(new FileWriter(new File(
					Constants.OIE_DATA_PATH).getParent()
					+ SIDE_PROP_CORRELATION));

			writerCounts = new BufferedWriter(new FileWriter(new File(
					Constants.OIE_DATA_PATH).getParent() + SIDE_PROP_COUNTS));

		} catch (IOException e) {
			e.printStackTrace();
		}

		for (String line : oieTriples) {
			elem = line.split(Constants.OIE_DATA_SEPERARTOR);
			oieSub = elem[0];
			oieRel = elem[1];
			oieObj = elem[2];
			ctr++;
			// process only the properties valid for the workflow
			if (propMap.containsKey(oieRel)) {
				try {
					// get the top-k concepts, confidence pairs
					// UTF-8 at this stage
					if (!CACHE.containsKey(oieSub)) {
						subCands = DBWrapper.fetchTopKLinksWikiPrepProb(
								Utilities.cleanse(oieSub).replaceAll("\\_+",
										" "), 1);
						if (subCands != null && subCands.size() > 0) {
							kbSub = subCands.get(0).split("\t")[0];
							CACHE.put(oieSub, kbSub);
						} else
							CACHE.put(oieSub, null);
					} else
						kbSub = CACHE.get(oieSub);

					if (!CACHE.containsKey(oieObj) && kbSub != null) {
						objCands = DBWrapper.fetchTopKLinksWikiPrepProb(
								Utilities.cleanse(oieObj).replaceAll("\\_+",
										" "), 1);
						if (objCands != null && objCands.size() > 0) {
							kbObj = objCands.get(0).split("\t")[0];
							CACHE.put(oieObj, kbObj);
						} else
							CACHE.put(oieObj, null);
					} else
						kbObj = CACHE.get(oieObj);

					if (kbSub != null && kbObj != null) {
						domainSideProps = createDistributionOnSideProperties(Utilities
								.utf8ToCharacter(kbSub));
						if (domainSideProps.size() > 0)
							rangeSideProps = createDistributionOnSideProperties(Utilities
									.utf8ToCharacter(kbObj));

						if (domainSideProps.size() > 0
								&& rangeSideProps.size() > 0) {
							calculateCorrelation(domainSideProps,
									rangeSideProps, oieRel, writer);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			// if (ctr == 5000)
			// break;

			if (ctr % 1000 == 0 && ctr > 1000)
				logger.info("Completed " + (double) ctr * 100
						/ oieTriples.size());
		}

		try {
			for (Entry<String, Map<Pair<String, String>, Long>> e1 : COUNT_MAP
					.entrySet()) {
				for (Entry<Pair<String, String>, Long> e2 : e1.getValue()
						.entrySet()) {

					writerCounts.write(e1.getKey() + "\t"
							+ e2.getKey().getLeft() + "\t"
							+ e2.getKey().getRight() + "\t" + e2.getValue()
							+ "\n");
				}
				writerCounts.flush();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			writer.close();
			writerCounts.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			DBWrapper.shutDown();
		}
	}

	/**
	 * retrieve the relations which would be eventually used in the workflow
	 */
	private static void getValidProps() {
		List<String> props = Generator.getOIERelations(-1,
				Long.parseLong(Constants.INSTANCE_THRESHOLD));

		for (String prop : props) {
			propMap.put(prop, 1L);
		}
	}

	/**
	 * write out the possible side properties
	 * 
	 * @param domainSideProps
	 * @param rangeSideProps
	 * @param oieRel
	 * @param writer
	 */
	private static void calculateCorrelation(List<String> domainSideProps,
			List<String> rangeSideProps, String oieRel, BufferedWriter writer) {

		double val1 = 0;
		double val2 = 0;
		double correlation = 0;

		String query = null;

		RealMatrix matrix = null;
		PearsonsCorrelation pearCorr = new PearsonsCorrelation();
		int rowCtr = 0;
		Pair<String, String> pair = null;

		for (String domSideProp : domainSideProps) {
			for (String ranSideProp : rangeSideProps) {
				pair = new ImmutablePair<String, String>(domSideProp,
						ranSideProp);

				if (!CACHE_PAIRS_PEARSON.containsKey(pair)) {

					updateCount(oieRel, pair);

					rowCtr = 0;
					query = "select ?sub ?obj where {?S ?P ?O. ?S <"
							+ domSideProp + "> ?sub. ?O <" + ranSideProp
							+ "> ?obj} limit 500";

					List<QuerySolution> list = SPARQLEndPointQueryAPI
							.queryDBPediaEndPoint(query);

					try {
						matrix = new Array2DRowRealMatrix(list.size(), 2);

						for (QuerySolution querySol : list) {

							try {
								val1 = Double.parseDouble(StringUtils
										.substringBefore(StringUtils
												.substringBefore(
														querySol.get("sub")
																.toString(),
														"^"), "-"));

								val2 = Double.parseDouble(StringUtils
										.substringBefore(StringUtils
												.substringBefore(
														querySol.get("obj")
																.toString(),
														"^"), "-"));

								matrix.addToEntry(rowCtr, 0, val1);
								matrix.addToEntry(rowCtr, 1, val2);
								rowCtr++;
							} catch (Exception e) {
							}
						}

						correlation = pearCorr.computeCorrelationMatrix(matrix)
								.getEntry(0, 1);
						CACHE_PAIRS_PEARSON.put(pair, correlation);

					} catch (Exception e) {
					}
				} else {
					correlation = CACHE_PAIRS_PEARSON.get(pair);
				}

				try {
					writer.write(oieRel + "\t" + domSideProp + "\t"
							+ ranSideProp + "\t" + correlation + "\n");

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		try {
			writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// /**
	// * @param domSideProp
	// * @param ranSideProp
	// * @return
	// */
	// private static boolean canBeCompared(String domSideProp, String
	// ranSideProp) {
	// String domType = null;
	// String ranType = null;
	//
	// Pair pair = new ImmutablePair<String, String>(domSideProp, ranSideProp);
	//
	// if (CACHE_PAIRS_PEARSON.containsKey(pair)) {
	// return CACHE_PAIRS_PEARSON.get(pair);
	// } else {
	// String query = "select distinct ?val where {<" + domSideProp
	// + "> <http://www.w3.org/2000/01/rdf-schema#range> ?val}";
	//
	// List<QuerySolution> list = SPARQLEndPointQueryAPI
	// .queryDBPediaEndPoint(query);
	// if (list != null && list.size() > 0) {
	// for (QuerySolution querySol : list) {
	// domType = querySol.get("val").toString();
	// }
	// }
	//
	// query = "select distinct ?val where {<" + ranSideProp
	// + "> <http://www.w3.org/2000/01/rdf-schema#range> ?val}";
	//
	// list = SPARQLEndPointQueryAPI.queryDBPediaEndPoint(query);
	// if (list != null && list.size() > 0) {
	// for (QuerySolution querySol : list) {
	// ranType = querySol.get("val").toString();
	// }
	// }
	//
	// if (domType != null && ranType != null && domType.equals(ranType)) {
	// // && domType.equals("http://www.w3.org/2001/XMLSchema#date")) {
	//
	// CACHE_PAIRS_PEARSON.put(new ImmutablePair<String, String>(
	// domSideProp, ranSideProp), true);
	// return true;
	// }
	//
	// CACHE_PAIRS_PEARSON.put(new ImmutablePair<String, String>(
	// domSideProp, ranSideProp), false);
	// return false;
	// }
	// }

	private static void updateCount(String oieRel, Pair<String, String> pair) {

		long val = 0;
		Map<Pair<String, String>, Long> map = null;
		if (!COUNT_MAP.containsKey(oieRel))
			map = new HashMap<Pair<String, String>, Long>();
		else
			map = COUNT_MAP.get(oieRel);

		if (!map.containsKey(pair))
			val = 1L;
		else {
			val = map.get(pair);
			val = val + 1;
		}

		map.put(pair, val);
		COUNT_MAP.put(oieRel, map);
	}

	/**
	 * retrieve the list of numerical properties for a given instance. These
	 * eventually form the side properties for a given relation
	 * 
	 * @param kbInst
	 * @return
	 */
	private static List<String> createDistributionOnSideProperties(String kbInst) {
		String sideProp = null;
		List<String> sideProps = new ArrayList<String>();

		String query = "select distinct ?sideProp where {?kbInst ?sideProp ?b. ?sideProp <http://www.w3.org/2000/01/rdf-schema#range> ?dataType. "
				+ "FILTER regex(str(?dataType), \"XMLSchema\"). "
				+ "FILTER(?dataType != <http://www.w3.org/2001/XMLSchema#string>). "
				+ "FILTER (!regex(str(?sideProp), \"wiki\", \"i\"))}";

		ParameterizedSparqlString sidePropQuery = new ParameterizedSparqlString(
				query);

		sidePropQuery
				.setIri("?kbInst", "http://dbpedia.org/resource/" + kbInst);
		// logger.info(sidePropQuery.toString());

		List<QuerySolution> list = SPARQLEndPointQueryAPI
				.queryDBPediaEndPoint(sidePropQuery.toString());
		if (list != null && list.size() > 0) {
			for (QuerySolution querySol : list) {
				sideProp = querySol.get("sideProp").toString();
				sideProps.add(sideProp);
			}
		}

		return sideProps;
	}

}
