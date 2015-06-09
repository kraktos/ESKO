/**
 * 
 */
package code.dws.setup;

import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.THashSet;
import gnu.trove.set.hash.TLinkedHashSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.lang3.tuple.ImmutablePair;
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
public class Generator {

	// define class logger
	private final static Logger logger = LoggerFactory
			.getLogger(Generator.class);

	private static Map<String, Long> COUNT_PROPERTY_INST = new HashMap<String, Long>();

	private static Map<String, THashSet<ImmutablePair<String, String>>> PROPS_INSTANCES_MAP = new HashMap<String, THashSet<ImmutablePair<String, String>>>();

	/**
	 * 
	 */
	public Generator() {

	}

	public static Map<String, THashSet<ImmutablePair<String, String>>> getPropInstance() {
		return PROPS_INSTANCES_MAP;
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
	public static List<String> getReverbProperties(int topKRevbProps,
			Long atLeastInstancesCount, boolean loadInstances) {

		String line = null;
		String[] arr = null;
		long val = 0;
		int c = 0;
		List<String> ret = new ArrayList<String>();
		THashSet<ImmutablePair<String, String>> list = null;

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

				if (PROPS_INSTANCES_MAP.containsKey(arr[1])) {
					list = PROPS_INSTANCES_MAP.get(arr[1]);
				} else {
					list = new TLinkedHashSet<ImmutablePair<String, String>>();
				}
				list.add(new ImmutablePair<String, String>(arr[0], arr[2]));
				PROPS_INSTANCES_MAP.put(arr[1], list);
			}

			// load the properties with atleast atLeastInstancesCount instances
			// each
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

	/**
	 * get the list of Reverb properties
	 * 
	 * Can be used to get both top-k properties, or properties with atleast
	 * atLeastInstancesCount number of instances
	 * 
	 * 
	 * @param topKOIERelations
	 * @param atLeastInstancesCount
	 * 
	 * @return List of properties
	 */
	@SuppressWarnings("unchecked")
	public static List<String> getOIERelations(int topKOIERelations,
			Long atLeastInstancesCount) {

		String line = null;
		String[] arr = null;
		long val = 0;
		int c = 0;
		List<String> ret = new ArrayList<String>();
		THashSet<ImmutablePair<String, String>> list = null;

		COUNT_PROPERTY_INST = new THashMap<String, Long>();

		try {
			@SuppressWarnings("resource")
			Scanner scan = new Scanner(new File(Constants.OIE_DATA_PATH));

			logger.info("Loaded " + Constants.OIE_DATA_PATH
					+ ", reading now properties");

			while (scan.hasNextLine()) {
				line = scan.nextLine();
				arr = line.split(Constants.OIE_DATA_SEPERARTOR);
				if (COUNT_PROPERTY_INST.containsKey(arr[1])) {
					val = COUNT_PROPERTY_INST.get(arr[1]);
					val++;
				} else {
					val = 1;
				}
				COUNT_PROPERTY_INST.put(arr[1], val);

				if (PROPS_INSTANCES_MAP.containsKey(arr[1])) {
					list = PROPS_INSTANCES_MAP.get(arr[1]);
				} else {
					list = new TLinkedHashSet<ImmutablePair<String, String>>();
				}
				list.add(new ImmutablePair<String, String>(arr[0], arr[2]));
				PROPS_INSTANCES_MAP.put(arr[1], list);
			}

			// load the properties with atleast atLeastInstancesCount instances
			// each
			COUNT_PROPERTY_INST = Utilities.sortByValue(COUNT_PROPERTY_INST,
					atLeastInstancesCount);

			for (Entry<String, Long> e : COUNT_PROPERTY_INST.entrySet()) {
				ret.add(e.getKey());

				c++;
				if (topKOIERelations != -1 && c == topKOIERelations)
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
