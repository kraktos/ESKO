/**
 * 
 */
package code.dws.core.knowGen;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import code.dws.core.cluster.analysis.ClusterAnalyzer;
import code.dws.core.propertyMap.RegressionAnalysis;
import code.dws.dbConnectivity.DBWrapper;
import code.dws.utils.Constants;
import code.dws.utils.FileUtil;
import code.dws.utils.Utilities;

/**
 * Main class for workflow 2. reads the cluster info, finds mapping factors for
 * each factor and tries to generate new triples
 * 
 * @author adutta
 * 
 */
public class GenerateTriples {

	private static final String NEW_TRIPLES = "/NEW_TRIPLES_REVERB_WF_";

	private static final String DISTRIBUTION_NEW_TRIPLES = "/NEW_TRIPLES_REVERB_DOM_RAN_WF_";

	/**
	 * logger
	 */
	// define Logger
	public static Logger logger = Logger.getLogger(GenerateTriples.class
			.getName());

	private static String directory;

	private static Map<String, List<String>> FINAL_MAPPINGS;

	/**
	 * 
	 */
	public GenerateTriples() {

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (args.length != 1) {
			logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.knowGen.GenerateTriples CONFIG.cfg");
		} else {

			Constants.loadConfigParameters(new String[] { "", args[0] });

			// get the OIE File Path
			directory = new File(Constants.OIE_DATA_PATH).getParent()
					.toString();

			String optiClusterFilePath = ClusterAnalyzer
					.getOptimalClusterPath();

			// load the clusters in memory
			FINAL_MAPPINGS = ClusterAnalyzer
					.getKBRelMappings(optiClusterFilePath);

			logger.info("Generating new triples for Workflow 3 .. ");

			// skim through the OIE input data file and try mapping
			createNewTriples();
		}
	}

	/**
	 * USE THE MAPPED PROPERTY, AND MAPPED INSTANCES TO GENERATE NEW-TRIPLES
	 * FROM THE NON-MAPPED CASES
	 * 
	 * @param filePath
	 * @param clusterNames
	 * @throws IOException
	 */
	private static void createNewTriples() throws IOException {
		Map<String, String> CACHED_SUBCLASSES = new HashMap<String, String>();

		// nell property in concern
		String oieProp = null;

		// write transactions to the file for analysis
		BufferedWriter triplesWriter = new BufferedWriter(new FileWriter(
				directory + NEW_TRIPLES + Constants.WORKFLOW + ".tsv"));

		BufferedWriter statStriplesWriter = new BufferedWriter(new FileWriter(
				directory + DISTRIBUTION_NEW_TRIPLES + Constants.WORKFLOW
						+ ".tsv"));

		// read the file into memory
		ArrayList<ArrayList<String>> fMinusFile = FileUtil.genericFileReader(
				new FileInputStream(directory + "/fMinus.dat"),
				Constants.OIE_DATA_SEPERARTOR, false);

		// init DB for getting the most frequebt URI for the NELL terms

		// MOST FREQUENT CASE
		// DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		// REFINED CASE
		DBWrapper.init(Constants.GET_REFINED_MAPPINGS_SQL);

		
		CACHED_SUBCLASSES = Utilities.buildClassHierarchy();

		System.out.println("Size of CACHED_SUBCLASSES = "
				+ CACHED_SUBCLASSES.size());
		
		// iterate through them
		for (ArrayList<String> line : fMinusFile) {
			oieProp = line.get(1);

			if (line.size() == 4) {		
				if (FINAL_MAPPINGS.containsKey(oieProp)) {
					List<String> dbProps = FINAL_MAPPINGS.get(oieProp);

					RegressionAnalysis.reCreateTriples(dbProps, line,
							triplesWriter, statStriplesWriter,
							CACHED_SUBCLASSES);
				}
			}
		}

		triplesWriter.close();
		statStriplesWriter.close();
	}

}
