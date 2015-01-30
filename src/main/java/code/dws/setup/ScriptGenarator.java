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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.utils.Constants;

/**
 * @author arnab
 */
public class ScriptGenarator {

	// define class logger
	public final static Logger logger = LoggerFactory
			.getLogger(ScriptGenarator.class);

	static List<String> PROPS = new ArrayList<String>();

	private static final String SHELL_SCRIPT = "PIPELINE.sh";

	private static final int MAX_BOOT_ITER = 3;

	private static final String PIPELINE_NAME = "MAPPER.sh ";

	private static final String BOOTSTRAP_NAME = "BOOTSTRAPPER.sh ";

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		Constants.loadConfigParameters(new String[] { "", args[0] });

		// load the properties
		loadOIEProps(Constants.OIE_DATA_PATH);

		generateScript();
	}

	/**
	 * takes the bunch of properties or clusters and generates script for
	 * running instance mapping
	 * 
	 * @throws IOException
	 */
	private static void generateScript() throws IOException {

		String directory = new File(Constants.OIE_DATA_PATH).getParent()
				.toString();

		BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(
				directory + "/WF." + Constants.WORKFLOW + "." + SHELL_SCRIPT));

		scriptWriter.write("#!/bin/bash\n\n");

		for (String oieProp : PROPS) {

			int bootIter = 2;

			scriptWriter.write("sh ./" + PIPELINE_NAME + oieProp + "\n");
			while (bootIter != MAX_BOOT_ITER + 2) {
				scriptWriter.write("sh ./" + BOOTSTRAP_NAME + oieProp + " "
						+ bootIter++ + "\n");
			}
			scriptWriter.write("echo \"Done with complete reasoning of "
					+ oieProp + "\"\n\n");

		}

		logger.info("echo \"Done with " + PROPS.size() + " clusters\n");
		scriptWriter.flush();
		scriptWriter.close();
	}

	/**
	 * load the valid properties in which we are interested. for Nell its easy,
	 * for Reverb there can be different variants..clustered and non-clustered.
	 * This is determined by WORKFLOW field in CONFIG.cfg file
	 * 
	 * @param oieFilePath
	 */
	private static void loadOIEProps(String oieFilePath) {
		boolean flag = false;

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
				// try {
				// CompareClusters.main(new String[] { "" });
				// for (Map.Entry<String, List<String>> e : CompareClusters
				// .getCluster().entrySet()) {
				//
				// PROPS.add(e.getKey());
				// }
				// } catch (IOException e) {
				// logger.error(e.getMessage());
				// }
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
