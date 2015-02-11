/**
 * 
 */
package code.dws.setup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.core.cluster.analysis.ClusterAnalyzer;
import code.dws.utils.Constants;

/**
 * @author arnab
 */
public class ScriptGenarator {

	// define class logger
	public final static Logger logger = LoggerFactory
			.getLogger(ScriptGenarator.class);

	static List<String> PROPS = new ArrayList<String>();

	private static final String SHELL_SCRIPT = "src/main/resources/script/";

	private static final int MAX_BOOT_ITER = 3;

	private static final String PIPELINE_NAME = "MAPPER.sh ";

	private static final String BOOTSTRAP_NAME = "BOOTSTRAPPER.sh ";

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.setup.ScriptGenarator CONFIG.cfg <#machines>");
		} else {
			Constants.loadConfigParameters(new String[] { "", args[0] });

			// load the properties
			loadOIEProps(Constants.OIE_DATA_PATH);

			generateScript(Integer.parseInt(args[1]));
		}
	}

	/**
	 * takes the bunch of properties or clusters and generates script for
	 * running instance mapping.
	 * 
	 * @param numberOfMachines
	 * 
	 * @throws IOException
	 */
	private static void generateScript(int numberOfMachines) throws IOException {
		int bucket = 0;
		int count = 0;

		BufferedWriter scriptWrtr[] = new BufferedWriter[numberOfMachines];
		for (int k = 0; k < numberOfMachines; k++) {
			try {
				scriptWrtr[k] = new BufferedWriter(new FileWriter(SHELL_SCRIPT
						+ "WF." + Constants.WORKFLOW + "." + "PIPELINE.N"
						+ (k + 1) + ".sh"));

				// write the shell script header in each file
				scriptWrtr[k].write("#!/bin/bash\n\n");

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		for (String oieProp : PROPS) {

			bucket = count++ % numberOfMachines;

			try {

				int bootIter = 2;
				scriptWrtr[bucket].write("sh ./" + PIPELINE_NAME + oieProp
						+ "\n");
				while (bootIter != MAX_BOOT_ITER + 2) {
					scriptWrtr[bucket].write("sh ./" + BOOTSTRAP_NAME + oieProp
							+ " " + bootIter++ + "\n");
				}
				scriptWrtr[bucket]
						.write("echo \"Done with complete reasoning of "
								+ oieProp + "\"\n\n");

				scriptWrtr[bucket].flush();

			} catch (IOException e) {
				logger.error("Problem while writing to " + scriptWrtr[bucket]);
				e.printStackTrace();
			}
		}

		logger.info("echo \"Done with " + PROPS.size() + " clusters\n");

		for (int k = 0; k < numberOfMachines; k++) {
			try {
				scriptWrtr[k].close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * load the valid properties in which we are interested. for Nell its easy,
	 * for Reverb there can be different variants..clustered and non-clustered.
	 * This is determined by WORKFLOW field in CONFIG.cfg file
	 * 
	 * @param oieFilePath
	 */
	private static void loadOIEProps(String oieFilePath) {
		if (Constants.IS_NELL) {
			// // load the NELL file in memory as a collection
			// ArrayList<ArrayList<String>> nellFile =
			// FileUtil.genericFileReader(
			// GenerateNewProperties.class
			// .getResourceAsStream(oieFilePath), PATH_SEPERATOR,
			// false);
			//
			// // iterate the file
			// for (ArrayList<String> line : nellFile) {
			// oieProp = line.get(1);
			// if (!PROPS.contains(oieProp.trim()))
			// PROPS.add(oieProp);
			// }
			//
			// log.info("Loaded all properties from "
			// + GenerateNewProperties.NELL_FILE_PATH + ";  "
			// + PROPS.size());

		} else {

			if (Constants.WORKFLOW == 2) {

				String directory = new File(Constants.OIE_DATA_PATH)
						.getParent()
						+ "/clusters/cluster.beta."
						+ (int) (Constants.OPTI_BETA * 10)
						+ ".inf."
						+ Constants.OPTI_INFLATION + ".out";
				logger.info("Generating opti Clusters from " + directory);

				for (Entry<String, List<String>> e : ClusterAnalyzer
						.getOptimalCluster(directory).entrySet()) {
					PROPS.add(e.getKey());
				}
			} else if (Constants.WORKFLOW == 1) {
				List<String> props = Generator.getReverbProperties(-1,
						Long.parseLong(Constants.INSTANCE_THRESHOLD));
				for (String s : props) {
					PROPS.add(s.replaceAll("\\s+", "-"));
				}
			}
		}
	}
}
