/**
 * 
 */

package code.dws.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.query.SPARQLEndPointQueryAPI;

import com.hp.hpl.jena.query.QuerySolution;

/**
 * All different kinds of utility methods are placed here
 * 
 * @author Arnab Dutta
 */
public class Utilities {
	// define Logger
	private static Logger logger = Logger.getLogger(Utilities.class.getName());

	// define Logger

	/**
	 * @param start
	 *            the timer start point
	 * @param message
	 *            the message you want to display
	 */
	public static void endTimer(final long start, final String message) {
		long end = System.currentTimeMillis();
		long execTime = end - start;
		logger.info(message
				+ " "
				+ String.format("%02d ms",
						TimeUnit.MILLISECONDS.toMillis(execTime)));
	}

	/**
	 * @return the start point of time
	 */
	public static long startTimer() {
		return System.currentTimeMillis();
	}

	public static String cleanse(String arg) {

		if (arg.indexOf(":") != -1)
			arg = StringUtils.substringAfter(arg, ":");

		if (arg.length() == 1)
			arg = arg.replaceAll("^(A|a)", "");
		else
			arg = arg.replaceAll("^(A |a )", "");

		return arg.toLowerCase();
	}

	/**
	 * encodes a string with special character to one with UTF-8 encoding
	 * 
	 * @param arg
	 * @return
	 */
	public static String characterToUTF8(String arg) {
		try {
			if (arg == null)
				return arg;
			return URLEncoder.encode(arg, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			logger.info("Exception while encoding");
		}
		return arg;
	}

	/**
	 * decodes a string with UTF-8 encoding to special character
	 * 
	 * @param arg
	 * @return
	 */
	public static String utf8ToCharacter(String arg) {
		try {
			if (arg == null)
				return arg;
			return URLDecoder.decode(arg, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			logger.info("Exception while dencoding");
			e.printStackTrace();
		} catch (IllegalArgumentException e2) {
			logger.info("Exception while dencoding");
			e2.printStackTrace();
		}
		return arg;
	}

	/**
	 * converts a probability to weights with smoothing
	 * 
	 * @param prob
	 * @return
	 */
	public static double convertProbabilityToWeight(double prob) {
		if (Constants.USE_LOGIT) {
			// smoothing
			if (prob >= 1)
				prob = 1 - Math.pow(10, -6);
			if (prob <= 0)
				prob = 0 + Math.pow(10, -6);

			return Constants.SCALE_WEIGHT + Math.log(prob / (1 - prob));
		} else
			return prob;
	}

	// ***************************************************************
	/**
	 * removes the DBpedia header uri information and cleanes the concept from
	 * any special character by converting it to to UTF-8
	 * 
	 * @param arg
	 * @return
	 */
	public static String cleanDBpediaURI(String arg) {
		return arg.replaceAll(Constants.DBPEDIA_PREDICATE_NS, "")
				.replaceAll(Constants.DBPEDIA_INSTANCE_NS, "")
				.replaceAll("\"", ""); // TODO
		// replaceAll(":_", "__")
	}

	public static String cleanseInstances(String dbpInst) {
		dbpInst = dbpInst.replaceAll("~", "%");
		dbpInst = dbpInst.replaceAll("\\[", "(");
		dbpInst = dbpInst.replaceAll("\\]", ")");
		dbpInst = dbpInst.replaceAll("\\*", "'");
		return utf8ToCharacter(dbpInst);
	}

	/**
	 * sort a map by value descending
	 * 
	 * @param map
	 * @param totalScore
	 * @param tripleCounter
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Map<String, Long> sortByValue(Map map) {
		List list = new LinkedList(map.entrySet());
		Collections.sort(list, new Comparator() {
			public int compare(Object o2, Object o1) {
				return ((Comparable) ((Map.Entry) (o1)).getValue())
						.compareTo(((Map.Entry) (o2)).getValue());
			}
		});

		Map result = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Long> entry = (Map.Entry) it.next();
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Map sortByValue(Map map, Long cutOff) {
		List list = new LinkedList(map.entrySet());
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o2)).getValue())
						.compareTo(((Map.Entry) (o1)).getValue());
			}
		});

		Map result = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Long> entry = (Map.Entry) it.next();
			if (entry.getValue() >= cutOff)
				result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public static Map<Pair<String, String>, Double> sortByValue(Map<?, ?> map,
			double cutOff) {
		List<Object> list = new LinkedList<Object>(map.entrySet());
		Collections.sort(list, new Comparator<Object>() {
			@SuppressWarnings("rawtypes")
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o2)).getValue())
						.compareTo(((Map.Entry) (o1)).getValue());
			}
		});

		Map<Pair<String, String>, Double> result = new LinkedHashMap<Pair<String, String>, Double>();
		for (Iterator<Object> it = list.iterator(); it.hasNext();) {
			Map.Entry<Pair<String, String>, Double> entry = (Entry<Pair<String, String>, Double>) it
					.next();
			if (result.size() < cutOff)
				result.put(entry.getKey(), entry.getValue());
			else
				return result;
		}
		return result;
	}

	/**
	 * split a word at capitals, needed for DBP relations
	 * 
	 * @param arg
	 * @return
	 */
	public static String splitAtCapitals(String arg) {
		String retStr = "";
		for (int i = 0; i < arg.length(); i++) {
			char c = arg.charAt(i);
			if (Character.isUpperCase(c)) {
				retStr = retStr + " " + c;
			} else {
				retStr = retStr + c;
			}
		}
		return retStr;
	}

	/**
	 * get the actual nell instance, following the ":" if any
	 * 
	 * @param arg
	 * @param identifier
	 * @return
	 */
	public static String getInst(String arg) {

		if (arg.indexOf(":") != -1)
			return arg.substring(arg.indexOf(":") + 1, arg.length());
		else
			return arg;
	}

	/**
	 * cleans of the "<" or ">" on the concepts
	 * 
	 * @param arg
	 *            value to be cleaned
	 * @return
	 */
	public static String removeTags(String arg) {

		arg = StringUtils.replace(arg, "_:", "");
		arg = StringUtils.replace(arg, "<", "");
		arg = StringUtils.replace(arg, ">\\)", "");
		arg = StringUtils.replace(arg, ">", "");
		arg = StringUtils.replace(arg, ",", "~2C");
		arg = StringUtils.replace(arg, "'", "*");
		arg = StringUtils.replace(arg, "%", "~");
		arg = StringUtils.replace(arg, "~28", "[");
		arg = StringUtils.replace(arg, "~29", "]");
		arg = StringUtils.replace(arg, "~27", "*");
		arg = StringUtils.replace(arg, "Node\\(", "");
		arg = StringUtils.replace(arg, "\\)", "]");
		arg = StringUtils.replace(arg, "\\(", "[");
		arg = StringUtils.replace(arg, "http://dbpedia.org/", "DBP#");
		arg = StringUtils.replace(arg, "\\(", "[");
		arg = StringUtils.replace(arg, "http://dws/OIE", "NELL");

		// arg = arg.replaceAll("_:", "");
		// arg = arg.replaceAll("<", "");
		// arg = arg.replaceAll(">\\)", "");
		// arg = arg.replaceAll(">", "");

		// arg = arg.replaceAll(",", "~2C");
		// arg = arg.replaceAll("'", "*");
		// arg = arg.replaceAll("%", "~");
		//
		// arg = arg.replaceAll("~28", "[");
		// arg = arg.replaceAll("~29", "]");
		// arg = arg.replaceAll("~27", "*");
		//
		// arg = arg.replaceAll("Node\\(", "");
		// arg = arg.replaceAll("\\)", "]");
		// arg = arg.replaceAll("\\(", "[");
		// arg = arg.replaceAll("http://dbpedia.org/", "DBP#");
		// arg = arg.replaceAll("http://dws/OIE", "NELL");
		return "\"" + arg.trim() + "\"";
	}

	public static String format(String arg) {
		arg = StringUtils.replace(arg, ",", "~2C");
		arg = StringUtils.replace(arg, "\\$", "~24");
		arg = StringUtils.replace(arg, "%", "~25");
		return arg;
	}

	/**
	 * builds a hierarchy of DBPedia concepts, looking into is subClassOf
	 * relation
	 * 
	 * @return
	 */
	public static Map<String, String> buildRelationHierarchy() {
		Map<String, String> CACHED_SUBCLASSES = new HashMap<String, String>();

		String getAll = "SELECT * WHERE  { ?subclass <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?superclass}";
		List<QuerySolution> allPairs = SPARQLEndPointQueryAPI
				.queryDBPediaEndPoint(getAll);

		for (QuerySolution querySol : allPairs) {
			// Get the next result row
			// QuerySolution querySol = results.next();
			// if (querySol.get("subclass").toString()
			// .indexOf(Constants.DBPEDIA_CONCEPT_NS) != -1
			// && querySol.get("superclass").toString()
			// .indexOf(Constants.DBPEDIA_CONCEPT_NS) != -1) {

			CACHED_SUBCLASSES.put(
					querySol.get("subclass").toString()
							.replaceAll(Constants.DBPEDIA_CONCEPT_NS, ""),
					querySol.get("superclass").toString()
							.replaceAll(Constants.DBPEDIA_CONCEPT_NS, ""));
			// }
		}

		logger.debug(CACHED_SUBCLASSES.toString());
		return CACHED_SUBCLASSES;
	}

	/**
	 * builds a hierarchy of DBPedia concepts, looking into is subClassOf
	 * relation
	 * 
	 * @return
	 */
	public static Map<String, String> buildClassHierarchy() {
		Map<String, String> CACHED_SUBCLASSES = new HashMap<String, String>();

		String getAll = "SELECT * WHERE  { ?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?superclass.FILTER(STRSTARTS(str(?subclass), 'http://dbpedia.org/ontology')).FILTER(STRSTARTS(str(?superclass), 'http://dbpedia.org/ontology'))}";
		List<QuerySolution> allPairs = SPARQLEndPointQueryAPI
				.queryDBPediaEndPoint(getAll);

		for (QuerySolution querySol : allPairs) {
			// Get the next result row
			// QuerySolution querySol = results.next();
			if (querySol.get("subclass").toString()
					.indexOf(Constants.DBPEDIA_CONCEPT_NS) != -1
					&& querySol.get("superclass").toString()
							.indexOf(Constants.DBPEDIA_CONCEPT_NS) != -1) {

				CACHED_SUBCLASSES.put(
						querySol.get("subclass").toString()
								.replaceAll(Constants.DBPEDIA_CONCEPT_NS, ""),
						querySol.get("superclass").toString()
								.replaceAll(Constants.DBPEDIA_CONCEPT_NS, ""));
			}
		}

		logger.debug(CACHED_SUBCLASSES.toString());
		return CACHED_SUBCLASSES;
	}

	public static List<String> getAllMyParents(String particularClass,
			List<String> coll, Map<String, String> CACHED_SUBCLASSES) {
		String superCls = CACHED_SUBCLASSES.get(particularClass);
		if (CACHED_SUBCLASSES.containsKey(superCls)) {
			coll.add(superCls);
			getAllMyParents(superCls, coll, CACHED_SUBCLASSES);
		} else {
			coll.add(superCls);
		}
		return coll;
	}

}
