/**
 * 
 */
package code.dws.setup;

import gnu.trove.map.hash.THashMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.utils.Constants;
import code.dws.utils.Utilities;

/**
 * A stand-alone class to generate required files for the whole workflow,
 * including data files
 * 
 * 
 * @author adutta
 *
 */
class Generator {

	// define class logger
	private final static Logger logger = LoggerFactory
			.getLogger(Generator.class);

	private static Map<String, Long> COUNT_PROPERTY_INST = new HashMap<String, Long>();

	/**
	 * 
	 */
	public Generator() {

	}

	/**
	 * get the list of Reverb properties
	 * 
	 * Can be used to get both top-k properties, or properties with atleast
	 * atLeastInstancesCount number of instances
	 * 
	 * 
	 * @param topKRevbProps
	 * @param atLeastInstancesCount
	 * 
	 * @return List of properties
	 */
	@SuppressWarnings("unchecked")
	static List<String> getReverbProperties(int topKRevbProps,
			Long atLeastInstancesCount) {

		String line = null;
		String[] arr = null;
		long val = 0;
		int c = 0;
		List<String> ret = new ArrayList<String>();
		COUNT_PROPERTY_INST = new THashMap<String, Long>();

		try {
			@SuppressWarnings("resource")
			Scanner scan = new Scanner(new File(Constants.OIE_DATA_PATH));

			logger.info("Loaded " + Constants.OIE_DATA_PATH
					+ ", reading now properties");

			while (scan.hasNextLine()) {
				line = scan.nextLine();
				arr = line.split(";");
				if (COUNT_PROPERTY_INST.containsKey(arr[1])) {
					val = COUNT_PROPERTY_INST.get(arr[1]);
					val++;
				} else {
					val = 1;
				}
				COUNT_PROPERTY_INST.put(arr[1], val);
			}

			// load the properties with atleast 500 instances each
			COUNT_PROPERTY_INST = Utilities.sortByValue(COUNT_PROPERTY_INST,
					atLeastInstancesCount);

			for (Entry<String, Long> e : COUNT_PROPERTY_INST.entrySet()) {
				ret.add(e.getKey());

				c++;
				if (topKRevbProps != -1 && c == topKRevbProps)
					return ret;
			}
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			COUNT_PROPERTY_INST.clear();
			COUNT_PROPERTY_INST = null;
		}
		return ret;
	}

}
