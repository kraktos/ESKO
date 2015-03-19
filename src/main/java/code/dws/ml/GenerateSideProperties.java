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

import org.apache.commons.io.FileUtils;
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

	public static final String FILE_FOR_SIDE_PROPS_DISTRIBUTION = "/REVERB_NUMERIC_SIDE_PROPS.tsv";

	/**
	 * logger
	 */
	public final static Logger logger = LoggerFactory
			.getLogger(GenerateSideProperties.class);

	static Map<String, Long> propMap = new HashMap<String, Long>();

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

		List<String> subCands;
		List<String> objCands;
		List<String> domainSideProps;
		List<String> rangeSideProps;

		BufferedWriter writer = null;

		// init DB
		DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		getValidProps();

		try {
			writer = new BufferedWriter(new FileWriter(new File(
					Constants.OIE_DATA_PATH).getParent()
					+ FILE_FOR_SIDE_PROPS_DISTRIBUTION));
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
					// logger.info(oieSub + "\t" + kbSub + "\t" + oieRel + "\t"
					// + oieObj + "\t" + kbObj);
					domainSideProps = createDistributionOnSideProperties(Utilities
							.utf8ToCharacter(kbSub));
					rangeSideProps = createDistributionOnSideProperties(Utilities
							.utf8ToCharacter(kbObj));
					if (domainSideProps.size() > 0 && rangeSideProps.size() > 0) {
						checkCompatibility(domainSideProps, rangeSideProps,
								oieRel, writer);
					}
				}
			}

			if (ctr % 1000 == 0 && ctr > 1000)
				logger.info("Completed " + (double) ctr * 100
						/ oieTriples.size());
		}

		try {
			writer.close();
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
		List<String> props = Generator.getReverbProperties(-1,
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
	private static void checkCompatibility(List<String> domainSideProps,
			List<String> rangeSideProps, String oieRel, BufferedWriter writer) {
		for (String domSideProp : domainSideProps) {
			for (String ranSideProp : rangeSideProps) {
				if (canBeCompared(domSideProp, ranSideProp)) {
					try {
						writer.write(oieRel + "\t" + domSideProp + "\t"
								+ ranSideProp + "\n");
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

		try {
			writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param domSideProp
	 * @param ranSideProp
	 * @return
	 */
	private static boolean canBeCompared(String domSideProp, String ranSideProp) {
		String domType = null;
		String ranType = null;

		String query = "select distinct ?val where {<" + domSideProp
				+ "> <http://www.w3.org/2000/01/rdf-schema#range> ?val}";

		List<QuerySolution> list = SPARQLEndPointQueryAPI
				.queryDBPediaEndPoint(query);
		if (list != null && list.size() > 0) {
			for (QuerySolution querySol : list) {
				domType = querySol.get("val").toString();
			}
		}

		query = "select distinct ?val where {<" + ranSideProp
				+ "> <http://www.w3.org/2000/01/rdf-schema#range> ?val}";

		list = SPARQLEndPointQueryAPI.queryDBPediaEndPoint(query);
		if (list != null && list.size() > 0) {
			for (QuerySolution querySol : list) {
				ranType = querySol.get("val").toString();
			}
		}

		if (domType.equals(ranType)
				&& domType.equals("http://www.w3.org/2001/XMLSchema#date"))
			return true;

		return false;
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

		// logger.info(query);
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
