/**
 * 
 */
package code.dws.statistics;

import gnu.trove.map.hash.THashMap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.setup.Generator;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

import com.hp.hpl.jena.query.QuerySolution;

/**
 * A gold standard creation for property mapping
 * 
 * @author adutta
 * 
 */
public class DataAnalyzer {

	// define class logger
	public final static Logger logger = LoggerFactory
			.getLogger(DataAnalyzer.class);

	private static String OIE_FILE_PATH = null;
	private static Map<String, Long> COUNT_PROPERTY_INST = new HashMap<String, Long>();
	// private static THashMap<String, Long> EMPTY_PROPERTY_MAP = new
	// THashMap<String, Long>();
	private static THashMap<Long, Long> COUNT_FREQUENY = new THashMap<Long, Long>();

	// number of gold standard facts
	private static final int SIZE = 10000;

	/**
	 * 
	 */
	public DataAnalyzer() {

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (args.length != 1) {
			logger.error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.statistics.DataAnalyzer CONFIG.cfg");
		} else {
			// load constants file
			Constants.loadConfigParameters(new String[] { "", args[0] });

			// READ THE INPUT RAW FILE AND FETCH THE PROPERTIES with atleast k
			// instances
			Generator.getOIERelations(-1,
					Long.parseLong(Constants.INSTANCE_THRESHOLD));
			COUNT_PROPERTY_INST = Generator.getRelationOccurrenceCountMap();

			logger.info("Distinct Properties in data set = "
					+ COUNT_PROPERTY_INST.size());

			doFrequencyAnalysis();

			logger.info("Loaded " + COUNT_PROPERTY_INST.size() + " properties");

			// read the file again to randomly select from those finally
			// filtered
			// property
			// createGoldStandard();

			// for (Entry<String, Long> e : EMPTY_PROPERTY_MAP.entrySet()) {
			// logger.info(e.getKey() + "\t" + e.getValue() + "\t"
			// + COUNT_PROPERTY_INST.get(e.getKey()));
			// }

		}
	}

	/**
	 * COUNT_PROPERTY_INST contains "relation => occurrence count". From this we
	 * need to create a collection with
	 * "count occurrence => how many such relations". Its like saying, how many
	 * such relations are there having x instances each.
	 * 
	 */
	private static void doFrequencyAnalysis() throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				Constants.OIE_DATA_PATH).getParent()
				+ "/oie.properties.instances.distribution.tsv"));

		Long occCount;

		long sum = 0;

		for (Entry<String, Long> e : COUNT_PROPERTY_INST.entrySet()) {
			// used for parity check. sum should be total instances
			sum = sum + e.getValue();

			if (COUNT_FREQUENY.containsKey(e.getValue())) {
				occCount = COUNT_FREQUENY.get(e.getValue());
				occCount = occCount + 1;
			} else {
				occCount = 1L;
			}

			COUNT_FREQUENY.put(e.getValue(), occCount);
		}

		logger.info("Number of instances = " + sum);

		long c = 0;
		writer.write("IC\n");
		for (Entry<Long, Long> e : COUNT_FREQUENY.entrySet()) {
			c = 0;
			while (c != e.getValue().longValue()) {
				writer.write(e.getKey() + "\n");
				c++;
			}
			// writer.write(e.getKey() + "\t" + e.getValue() + "\n");
		}
		writer.flush();
		writer.close();
	}

	/**
	 * 
	 * @throws IOException
	 */
	private static void createGoldStandard() throws IOException {
		String line = null;
		String[] arr = null;
		String oieSub = null;
		String oieProp = null;
		String oieObj = null;

		List<String> topkSubjects = null;
		List<String> topkObjects = null;
		List<String> lines = new ArrayList<String>();

		// writing annotation file to
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				OIE_FILE_PATH).getParent()
				+ "/test.gs.reverb.sample."
				+ SIZE
				+ ".csv"));

		// Reading from
		@SuppressWarnings("resource")
		Scanner scan = new Scanner(new File(OIE_FILE_PATH));

		// init DB
		DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		// select the lines from input relevant
		while (scan.hasNextLine()) {
			line = scan.nextLine();
			arr = line.split(";");
			oieProp = arr[1];

			// if this is the selected property, add it
			if (COUNT_PROPERTY_INST.containsKey(oieProp))
				lines.add(line);
		}

		// randomize the list so as to avoid one type of facts in contiguous
		// locations
		Collections.shuffle(lines);

		Random rand = new Random();

		Set<Integer> randomNumSet = new HashSet<Integer>();

		while (randomNumSet.size() < SIZE) {

			Integer randomNum = rand.nextInt(lines.size()) + 1;

			if (!randomNumSet.contains(randomNum)) {

				logger.debug("Reading line " + randomNum);

				line = lines.get(randomNum);

				arr = line.split(";");
				oieSub = Utilities.cleanse(arr[0]).replaceAll("\\_+", " ");
				oieProp = arr[1];
				oieObj = Utilities.cleanse(arr[2]).replaceAll("\\_+", " ");

				// get top-k candidates of the subject
				topkSubjects = DBWrapper.fetchTopKLinksWikiPrepProb(oieSub,
						Constants.TOP_K_MATCHES);

				// get the topk instances for oieObj
				topkObjects = DBWrapper.fetchTopKLinksWikiPrepProb(oieObj,
						Constants.TOP_K_MATCHES);

				if (!linkExists(topkSubjects, topkObjects)) {

					randomNumSet.add(randomNum);

					writer.write(oieSub + "\t" + oieProp + "\t" + oieObj + "\t"
							+ "?" + "\t" + "?" + "\t" + "?" + "\t" + "IP\n");
					ioRoutine(oieProp, topkSubjects, topkObjects, writer);
				}

				writer.write("\n");
				writer.flush();
			}

			if (randomNumSet.size() % 1000 == 0)
				logger.info("Completed " + 100
						* ((double) randomNumSet.size() / SIZE) + "%");
		}

		randomNumSet.clear();
		COUNT_PROPERTY_INST.clear();
		writer.close();
		DBWrapper.shutDown();
	}

	private static boolean linkExists(List<String> topkSubjects,
			List<String> topkObjects) {

		String candSubj = null;
		String candObj = null;
		String sparql = null;
		List<QuerySolution> s = null;

		if (topkSubjects == null || topkSubjects.size() == 0)
			return true;

		if (topkObjects == null || topkObjects.size() == 0)
			return true;

		for (String sub : topkSubjects) {
			for (String obj : topkObjects) {

				candSubj = Utilities.utf8ToCharacter(sub.split("\t")[0]);
				candObj = Utilities.utf8ToCharacter(obj.split("\t")[0]);

				try {
					sparql = "select * where {<"
							+ Constants.DBPEDIA_INSTANCE_NS
							+ candSubj
							+ "> ?val <"
							+ Constants.DBPEDIA_INSTANCE_NS
							+ candObj
							+ ">. "
							+ "?val <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>."
							+ "FILTER(!regex(str(?val), 'http://dbpedia.org/ontology/wikiPageWikiLink'))}";

					s = SPARQLEndPointQueryAPI.queryDBPediaEndPoint(sparql);

					if (s.size() > 0) {
						// logger.info(candSubj + "\t"
						// + s.get(0).get("val").toString() + "\t"
						// + candObj);
						return true;
					}
				} catch (Exception e) {
					continue;
				}
			}
		}
		return false;
	}

	/**
	 * @param oieProp
	 * @param topkSubjects
	 * @param topkObjects
	 * @param writer
	 * @throws IOException
	 */
	public static void ioRoutine(String oieProp, List<String> topkSubjects,
			List<String> topkObjects, BufferedWriter writer) throws IOException {

		if (topkSubjects.size() > 0 && topkObjects.size() > 0) {

			String candSub = null;
			String candObj = null;

			for (int j = 0; j < ((topkSubjects.size() > topkObjects.size()) ? topkSubjects
					.size() : topkObjects.size()); j++) {

				candSub = (j > topkSubjects.size() - 1) ? "-"
						: Constants.DBPEDIA_INSTANCE_NS
								+ topkSubjects.get(j).split("\t")[0];

				candObj = (j > topkObjects.size() - 1) ? "-"
						: Constants.DBPEDIA_INSTANCE_NS
								+ topkObjects.get(j).split("\t")[0];

				writer.write("\t" + "\t" + "\t"
						+ Utilities.utf8ToCharacter(candSub) + "\t" + "" + "\t"
						+ Utilities.utf8ToCharacter(candObj) + "\n");
			}
		}

		if (topkSubjects.size() > 0
				&& (topkObjects == null || topkObjects.size() == 0)) {
			for (String candSub : topkSubjects) {
				writer.write("\t" + "\t" + "\t" + Constants.DBPEDIA_INSTANCE_NS
						+ Utilities.utf8ToCharacter(candSub.split("\t")[0])
						+ "\t" + "-" + "\t" + "-" + "\n");
			}
		}
		if ((topkSubjects == null || topkSubjects.size() == 0)
				&& topkObjects != null) {
			for (String candObj : topkObjects) {
				writer.write("\t" + "\t" + "\t" + "-" + "\t" + "-" + "\t"
						+ Constants.DBPEDIA_INSTANCE_NS
						+ Utilities.utf8ToCharacter(candObj.split("\t")[0])
						+ "\n");
			}
		}
		if ((topkSubjects == null || topkSubjects.size() == 0)
				&& (topkObjects == null || topkObjects.size() == 0)) {
			writer.write("\t" + "\t" + "\t" + "-" + "\t" + "" + "\t" + "-"
					+ "\n");
		}
	}
}
