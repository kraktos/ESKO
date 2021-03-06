/**
 * 
 */
package code.dws.core.propertyMap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.log4j.Logger;

import code.dws.core.cluster.analysis.ClusterAnalyzer;
import code.dws.dbConnectivity.DBWrapper;
import code.dws.markovLogic.YagoDbpediaMapping;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.setup.Generator;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

import com.hp.hpl.jena.query.QuerySolution;

/**
 * This creates the domain/range restrictions for a given set of OIE relations.
 * The valid input file is the fPlus file, because thats where we can find a KB
 * assertion and domain/range values, fMinus is useless for the pattern learning
 * stuff.
 * 
 * @author Arnab Dutta
 */
public class GenerateAssociations {

	public static String DIRECTORY = null;

	public static String INVERSE_PROP_LOG = null;

	public static String DIRECT_PROP_LOG = null;

	// define Logger
	public static Logger logger = Logger.getLogger(GenerateAssociations.class
			.getName());

	private static boolean refinedMode = true;

	private static Map<String, List<String>> propertyClusterNames;
	private static List<String> propertyNames = new ArrayList<String>();

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (args.length != 2)
			logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.propertyMap.GenerateAssociations CONFIG.cfg <0 or 1>");
		else {
			init(args[0]);
			refinedMode = (args[1].equals("1")) ? true : false;
			findDomRanRestrictions(Constants.OIE_DATA_PATH);
		}
	}

	/**
	 * initializes the setup process, configuration params, loading etc
	 * 
	 * @throws IOException
	 * 
	 */
	public static void init(String configFile) throws IOException {

		Constants.loadConfigParameters(new String[] { "", configFile });

		// get the OIE File Path
		DIRECTORY = new File(Constants.OIE_DATA_PATH).getParent().toString();

		// Reverb has multiple wF options, hence it has been concatenated with
		// the file name, NELL doesnot (its just WF 1)		
		INVERSE_PROP_LOG = (Constants.IS_NELL) ? DIRECTORY
				+ "/INVERSE_PROP.log" : DIRECTORY + "/REVERB_INVERSE_PROP.WF."
				+ Constants.WORKFLOW + ".log";
		DIRECT_PROP_LOG = (Constants.IS_NELL) ? DIRECTORY + "/DIRECT_PROP.log"
				: DIRECTORY + "/REVERB_DIRECT_PROP.WF." + Constants.WORKFLOW
						+ ".log";

		// initiate yago info
		if (Constants.INCLUDE_YAGO_TYPES)
			YagoDbpediaMapping.main(new String[] { "" });

		if (Constants.WORKFLOW == 2 || Constants.WORKFLOW == 3) {
			String directory = ClusterAnalyzer.getOptimalClusterPath();

			// retrieve the properties relevant to the given cluster name
			propertyClusterNames = ClusterAnalyzer.getOptimalCluster(directory);

		} else { // normal workflow, without cluster
			propertyNames.addAll(Generator.getOIERelations(-1,
					Long.parseLong(Constants.INSTANCE_THRESHOLD)));
			logger.info("total properties added = " + propertyNames.size());
		}
	}

	/**
	 * read the OIE file to find the domain/range restrictions for each OIE
	 * relation
	 * 
	 * @param oieFilePath
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	public static void findDomRanRestrictions(String oieFilePath)
			throws IOException {

		int lineCounter = 0;

		String line = null;
		String oieRawSubj = null;
		String oieRawProp = null;
		String oieRawObj = null;

		String[] elems = null;

		List<String> candidates = null;
		List<String> candidateSubjs = null;
		List<String> candidateObjs = null;

		BufferedWriter directPropWriter = new BufferedWriter(new FileWriter(
				DIRECT_PROP_LOG));
		BufferedWriter inversePropWriter = new BufferedWriter(new FileWriter(
				INVERSE_PROP_LOG));

		// init DB for getting the most frequent URI for the NELL terms

		// DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);
		if (!refinedMode)
			DBWrapper.init(Constants.GET_TOP_1_TOGETHER_SQL);

		// another run mode with refined mapping, not just top-1
		if (refinedMode)
			DBWrapper.init(Constants.GET_REFINED_MAPPINGS_SQL);

		String directory = new File(oieFilePath).getParent().toString();

		// should read the fPlus file only, there's where domain/range values
		// can
		// be found, since they have
		// an analogous KB assertion
		Scanner scan = new Scanner(new File(directory + "/fPlus.dat"), "UTF-8");

		logger.info("Writing out direct Associations at " + DIRECT_PROP_LOG);
		logger.info("Writing out inverse Associations at " + INVERSE_PROP_LOG);

		// iterate the file
		while (scan.hasNextLine()) {
			line = scan.nextLine();

			elems = line.split(Constants.OIE_DATA_SEPERARTOR);
			try {
				logger.debug(elems[0] + "\t" + elems[1] + "\t" + elems[2]
						+ "\n");

				// get the oie subjects and objects
				oieRawSubj = elems[0].replaceAll("\\_+", " ");
				oieRawProp = elems[1];
				oieRawObj = elems[2].replaceAll("\\_+", " ");

				if (isRelevantProperty(oieRawProp)) {

					if (!refinedMode)
						candidates = DBWrapper.fetchTop1Mapping(Utilities
								.cleanse(oieRawSubj).trim(),
								Utilities.cleanse(oieRawObj).trim());
					else
						candidates = DBWrapper.fetchRefinedMapping(
								Utilities.cleanse(oieRawSubj).trim(),
								(Constants.IS_NELL) ? oieRawProp.trim()
										.replaceAll("\\s+", "_") : oieRawProp
										.trim(), Utilities.cleanse(oieRawObj)
										.trim());

					try {
						candidateSubjs = new ArrayList<String>();
						if (candidates != null && candidates.size() > 0)
							candidateSubjs.add(candidates.get(0));

						candidateObjs = new ArrayList<String>();
						if (candidates != null && candidates.size() > 0)
							candidateObjs.add(candidates.get(1));

					} catch (IndexOutOfBoundsException e) {
						logger.error(e.getMessage());
					}

					// use the SPARQL endpoint for querying the direct and
					// inverse relation between the sub-obj pairs
					findDirectIndirectProps(elems, candidateSubjs,
							candidateObjs, directPropWriter, inversePropWriter);

					// update GLOBAL_PROPERTY_MAPPINGS with the possible values
					// updateTheCollection(nellRawPred, directProperties);
				}
				if (lineCounter++ % 10000 == 0)
					logger.info("Completed " + lineCounter + " lines ");

			} catch (Exception e) {
				logger.error("Problem with line " + line.toString());
				e.printStackTrace();
				continue;
			}
		}

		// close streams
		directPropWriter.close();
		inversePropWriter.close();

	}

	/**
	 * often no need to look for every property in the data set..for reverb we
	 * are dealing with top-k properties..so makes it time efficient
	 * 
	 * @param oieRawProp
	 * @return
	 */
	private static boolean isRelevantProperty(String oieRawProp) {
		boolean flag = false;

		if (propertyNames.contains(oieRawProp)) {
			flag = true;
		} else {
			if (Constants.WORKFLOW == 2 || Constants.WORKFLOW == 3) {
				for (Entry<String, List<String>> clusterName : propertyClusterNames
						.entrySet()) {
					for (String relation : clusterName.getValue()) {
						if (!propertyNames.contains(relation)) {
							propertyNames.add(relation);

							if (relation.equals(oieRawProp))
								flag = true;
						}
					}
				}
			}
		}

		return flag;
	}

	/**
	 * this method takes the possible set of candidates and tries to find the
	 * connecting property path between them
	 * 
	 * @param line
	 * @param candidateSubj
	 * @param candidateObj
	 * @param directPropWriter
	 * @param inversePropWriter
	 * @return
	 * @throws IOException
	 */
	public static void findDirectIndirectProps(String[] line,
			List<String> candidateSubj, List<String> candidateObj,
			BufferedWriter directPropWriter, BufferedWriter inversePropWriter)
			throws IOException {

		boolean blankDirect = false;
		boolean blankInverse = false;
		String domainType = null;
		String rangeType = null;

		String domainTypeInv = null;
		String rangeTypeInv = null;

		List<String> directPropList = new ArrayList<String>();
		List<String> inversePropList = new ArrayList<String>();

		// for the current NELL predicate get the possible db:properties from
		// SPARQL endpoint

		for (String subjCand : candidateSubj) {
			if (!subjCand.equals("X")) {
				for (String objCand : candidateObj) {

					if (!objCand.equals("X")) {
						// DIRECT PROPERTIES
						String directProperties = getPredsFromEndpoint(
								subjCand.split("\t")[0], objCand.split("\t")[0]);

						if (directProperties.length() > 0) {
							directPropList.add(directProperties);

							// find domain type
							domainType = getTypeInfo(subjCand.split("\t")[0]);
							domainType = (domainType.length() == 0) ? "null"
									: domainType;

							// find range type
							rangeType = getTypeInfo(objCand.split("\t")[0]);
							rangeType = (rangeType.length() == 0) ? "null"
									: rangeType;

						}

						// INDIRECT PROPERTIES
						String inverseProps = getPredsFromEndpoint(
								objCand.split("\t")[0], subjCand.split("\t")[0]);

						if (inverseProps.length() > 0) {
							inversePropList.add(inverseProps);

							// find domain type
							if (rangeType != null) {
								domainTypeInv = rangeType;
							} else {
								domainTypeInv = getTypeInfo(objCand.split("\t")[0]);
								domainTypeInv = (domainTypeInv.length() == 0) ? "null"
										: domainTypeInv;
							}

							// find range type
							if (domainType != null) {
								rangeTypeInv = domainType;
							} else {
								rangeTypeInv = getTypeInfo(subjCand.split("\t")[0]);
								rangeTypeInv = (rangeTypeInv.length() == 0) ? "null"
										: rangeTypeInv;

							}
						}
					}
				}
			}
		}

		// write it out to log files
		if (directPropList.size() > 0) {
			blankDirect = true;
			directPropWriter.write(line[0] + "\t" + line[1] + "\t" + line[2]
					+ "\t");
			logger.debug(line + "\t");
			for (String elem : directPropList) {
				directPropWriter.write(elem + "\t");
				logger.debug(elem + "\t");
			}
			directPropWriter.write(domainType + "\t" + rangeType);

			directPropWriter.write("\n");
			logger.debug("\n");
			directPropWriter.flush();

		}

		if (inversePropList.size() > 0) {
			blankInverse = true;

			inversePropWriter.write(line[0] + "\t" + line[1] + "\t" + line[2]
					+ "\t");
			logger.debug(line + "\t");
			for (String elem : inversePropList) {
				inversePropWriter.write(elem + "\t");
				logger.debug(elem + "\t");
			}
			inversePropWriter.write(domainTypeInv + "\t" + rangeTypeInv);
			inversePropWriter.write("\n");
			logger.debug("\n");
			inversePropWriter.flush();
		}

		// if all possible candidate pairs have no predicates mapped, just
		// add one entry in each log file, not
		// multiple blank entries
		if (!blankDirect) {
			directPropWriter.write(line[0] + "\t" + line[1] + "\t" + line[2]
					+ "\n");
			directPropWriter.flush();
		}

		if (!blankInverse) {
			inversePropWriter.write(line[0] + "\t" + line[1] + "\t" + line[2]
					+ "\n");
			inversePropWriter.flush();
		}
	}

	@SuppressWarnings("finally")
	private static String getTypeInfo(String inst) {
		String mostSpecificVal = "";

		List<String> types = SPARQLEndPointQueryAPI.getInstanceTypes(Utilities
				.utf8ToCharacter(inst));

		try {
			mostSpecificVal = SPARQLEndPointQueryAPI.getLowestType(types)
					.get(0);
		} catch (IndexOutOfBoundsException e) {
		} finally {
			// return mostSpecificVal + "\t" + mostGeneralVal;
			return mostSpecificVal;
		}
	}

	/**
	 * get the possible predicates for a particular combination
	 * 
	 * @param candSubj
	 * @param candObj
	 * @return
	 */
	private static String getPredsFromEndpoint(String candSubj, String candObj) {
		StringBuffer sBuf = new StringBuffer();

		// remove all utf-8 characters and convert them to characters
		candSubj = Utilities.utf8ToCharacter(candSubj);
		candObj = Utilities.utf8ToCharacter(candObj);

		if (candSubj.endsWith("%"))
			candSubj = candSubj.replaceAll("%", "");

		if (candObj.endsWith("%"))
			candObj = candObj.replaceAll("%", "");

		// possible predicate variable
		String possiblePred = null;

		// return list of all possible predicates
		List<String> returnPredicates = new ArrayList<String>();

		String sparqlQuery = "select * where {<"
				+ Constants.DBPEDIA_INSTANCE_NS
				+ candSubj
				+ "> ?val <"
				+ Constants.DBPEDIA_INSTANCE_NS
				+ candObj
				+ ">. "
				+ "?val <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>."
				+ "FILTER(!regex(str(?val), 'http://dbpedia.org/ontology/wikiPageWikiLink'))}";

		logger.debug(sparqlQuery);

		// fetch the result set
		List<QuerySolution> listResults = SPARQLEndPointQueryAPI
				.queryDBPediaEndPoint(sparqlQuery);

		if (listResults != null) {
			for (QuerySolution querySol : listResults) {
				possiblePred = querySol.get("val").toString();

				// add the sub classes to a set
				returnPredicates.add(possiblePred);
				sBuf.append(possiblePred + "\t");
			}
		}

		return sBuf.toString().trim();

	}

}
