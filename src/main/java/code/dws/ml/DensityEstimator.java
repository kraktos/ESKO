/**
 * 
 */
package code.dws.ml;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.utils.Constants;

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

	static Map<String, Map<Pair<String, String>, Long>> GLBL_COLL = new HashMap<String, Map<Pair<String, String>, Long>>();

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
			print();

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

				writeOut(elem[0], new ImmutablePair<String, String>(elem[1],
						elem[2]));
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
}
