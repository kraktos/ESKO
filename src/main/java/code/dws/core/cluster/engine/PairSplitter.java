package code.dws.core.cluster.engine;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Logger;

import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.setup.Generator;
import code.dws.utils.Constants;

/**
 * this class creates pairs of files to be distributed across machines. Since
 * pairwise similarity computation is time consuming, makes sense to split and
 * compute and also exploiting the multi-cores of those individual machines.
 * 
 * @author adutta
 *
 */
public class PairSplitter {

	// define Logger
	public static Logger logger = Logger
			.getLogger(PairSplitter.class.getName());

	public static void main(String[] args) throws IOException {
		String dataType = null;
		int numberOfMachines = 0;

		if (args.length != 3) {
			logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.cluster.engine.PairSplitter CONFIG.cfg <type of file> <#machines>");
			logger.error("Options for type of File: OIE, KB, BOTH");
		} else {
			Constants.loadConfigParameters(new String[] { "", args[0] });

			dataType = args[1];
			numberOfMachines = Integer.parseInt(args[2]);

			if (dataType.toUpperCase().equals("KB")
					|| dataType.toUpperCase().equals("OIE")
					|| dataType.toUpperCase().equals("BOTH")) {
				split(dataType.toUpperCase(), numberOfMachines);

				// finally split for the machines
				readAndSplit(dataType.toUpperCase(), numberOfMachines);
			} else {
				logger.error("Invalid input parameters");
				logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.cluster.engine.PairSplitter CONFIG.cfg <type of file> <#machines>");
				logger.error("Options for type of File: OIE, KB, BOTH");
			}
		}

	}

	/**
	 * reads the generated single file, to split across different machines. This
	 * has to be done manually
	 * 
	 * @param numberOfMachines
	 * 
	 * @param string
	 */
	private static void readAndSplit(String dataType, int numberOfMachines) {
		int bucket = 0;
		int count = 0;

		String filePath = new File(Constants.OIE_DATA_PATH).getParent()
				+ "/pairNodeAll." + dataType + ".csv";

		BufferedWriter writerArr[] = new BufferedWriter[numberOfMachines];
		for (int k = 0; k < numberOfMachines; k++) {
			try {
				writerArr[k] = new BufferedWriter(new FileWriter(new File(
						Constants.OIE_DATA_PATH).getParent()
						+ "/pairNodeAll."
						+ dataType + ".Node." + (k + 1) + ".csv"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		Scanner scan = null;
		try {
			scan = new Scanner(new File(filePath), "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		logger.info("Splitting for " + numberOfMachines + " machines");
		while (scan.hasNextLine()) {
			bucket = count++ % numberOfMachines;

			try {

				writerArr[bucket].write(scan.nextLine());
				writerArr[bucket].flush();
			} catch (IOException e) {
				logger.error("Problem while writing to " + writerArr[bucket]);
				e.printStackTrace();
			}
		}

		for (int k = 0; k < numberOfMachines; k++) {
			try {
				writerArr[k].close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		logger.info("Done splitting");

	}

	/**
	 * determines the data type and splits them accordingly
	 * 
	 * @param dataTypeIdentifier
	 * @param numberOfMachines
	 */
	private static void split(String dataTypeIdentifier, int numberOfMachines) {
		List<String> dbpProps = null;
		List<String> revbProps = null;

		// perform steps for different cases and iterate to create pairwise file
		if (dataTypeIdentifier.equals("OIE")
				|| dataTypeIdentifier.equals("BOTH")) {
			try {
				revbProps = Generator.getReverbProperties(-1,
						Long.parseLong(Constants.INSTANCE_THRESHOLD));

				logger.info("Loaded " + revbProps.size() + " OIE properties");

			} catch (Exception e) {
			}
		}

		if (dataTypeIdentifier.equals("KB")
				|| dataTypeIdentifier.equals("BOTH")) {
			// call to retrieve DBPedia owl object property
			dbpProps = SPARQLEndPointQueryAPI.loadDbpediaProperties(-1,
					Constants.QUERY_OBJECTTYPE);
			logger.info("Loaded " + dbpProps.size() + " DBpedia properties");
		}

		try {
			if (dataTypeIdentifier.equals("KB")) {
				iterateCollection(dbpProps, null, dataTypeIdentifier);
			} else if (dataTypeIdentifier.equals("OIE")) {
				iterateCollection(revbProps, null, dataTypeIdentifier);
			} else {
				iterateCollection(revbProps, dbpProps, dataTypeIdentifier);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 
	 * write out the pairs
	 * 
	 * @param collArg1
	 * @param collArg2
	 * @param identifier
	 * @throws IOException
	 */
	private static void iterateCollection(List<String> collArg1,
			List<String> collArg2, String identifier) throws IOException {

		String arg1 = null;
		String arg2 = null;

		BufferedWriter pairNode = new BufferedWriter(new FileWriter(new File(
				Constants.OIE_DATA_PATH).getParent()
				+ "/pairNodeAll."
				+ identifier + ".csv"));

		logger.info("Writing at "
				+ new File(Constants.OIE_DATA_PATH).getParent()
				+ "/pairNodeAll." + identifier + ".csv");

		if (identifier.equals("OIE") || identifier.equals("KB")) {
			for (int outerIdx = 0; outerIdx < collArg1.size(); outerIdx++) {
				arg1 = collArg1.get(outerIdx);
				for (int innerIdx = outerIdx + 1; innerIdx < collArg1.size(); innerIdx++) {
					arg2 = collArg1.get(innerIdx);
					pairNode.write(arg1 + "\t" + arg2 + "\n");
				}
				pairNode.flush();
			}
		} else if (identifier.equals("BOTH")) {
			for (int outerIdx = 0; outerIdx < collArg1.size(); outerIdx++) {
				arg1 = collArg1.get(outerIdx);
				for (int innerIdx = 0; innerIdx < collArg2.size(); innerIdx++) {
					arg2 = collArg2.get(innerIdx);
					pairNode.write(arg1 + "\t" + arg2 + "\n");
				}
				pairNode.flush();
			}
		}

		pairNode.close();
		logger.info("Done..");

	}
}
