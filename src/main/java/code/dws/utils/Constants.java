/**
 * 
 */

package code.dws.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * This class stores a set of constants required for the application
 * 
 * @author Arnab Dutta
 */
public class Constants {

	public static enum OIE {
		NELL, REVERB
	}

	public static DecimalFormat formatter = new DecimalFormat(
			"###.############");

	public static long WORKFLOW = 0;

	public static String DBPEDIA_TBOX;

	public static String INSTANCE_THRESHOLD;

	/**
	 * Parameters
	 */
	public static int BATCH_SIZE = 2000;

	public static String PREDICATE = null;

	public static Double PROPGTN_FACTOR = 0D;

	public static Double SIMILARITY_FACTOR = 0D;

	public static int TOP_K_MATCHES = 1;

	public static int THREAD_MAX_POOL_SIZE = 0;

	public static String OIE_DATA_PATH;

	public static boolean IS_NELL;

	static boolean USE_LOGIT;

	public static boolean BOOTSTRAP;

	public static boolean RELOAD_TYPE;

	static int SCALE_WEIGHT;

	public static boolean ENGAGE_INTER_STEP;

	public static int OPTIMAL_INFLATION;

	public static int TOP_K_NUMERIC_PROPERTIES;

	public static boolean INCLUDE_YAGO_TYPES;

	// public static boolean WORKFLOW_NORMAL;

	public static final String DOMAIN = "Domain";

	public static final String RANGE = "Range";

	public static final String POST_FIX = "_";

	/**
	 * Substitute placeholder for missing type information
	 */
	public static final String UNTYPED = "UNTYPED";

	/**
	 * Namespaces
	 */

	public static final String YAGO_HEADER = "http://dbpedia.org/class/yago";

	public static String ONTOLOGY_NAMESPACE = "http://dbpedia.org/ontology/";

	private static String DBPEDIA_NAMESPACE = "http://dbpedia.org/";

	public static String DBPEDIA_INSTANCE_NS = DBPEDIA_NAMESPACE + "resource/";

	public static String DBPEDIA_CONCEPT_NS = DBPEDIA_NAMESPACE + "ontology/";

	public static String DBPEDIA_PREDICATE_NS = DBPEDIA_NAMESPACE + "ontology/";

	/**
	 * DBPedia End point URL
	 */
	public static String DBPEDIA_SPARQL_ENDPOINT_LOCAL = null;

	public static String DBPEDIA_SPARQL_ENDPOINT = null;

	public static String DBPEDIA_SPARQL_ENDPOINT_LIVE_DBP = null;

	public static long TIMEOUT_MINS = 0;

	// *****************DIRECTORY LOCATIONS
	// ************************************************

	/**
	 * file I/O location
	 */
	public static final String sample_dumps = "src/main/resources/output/ds_";

	public static String ALL_MLN_EVIDENCE;

	public static String BASIC_REASON_OUT_FILE;

	public static String DOMAIN_RANGE_PREFERENCE_FILE;

	public static String DOMAIN_RANGE_EVIDENCE_FILE;

	public static String DOMAIN_RANGE_BS_PREFERENCE_FILE;

	public static String DOMAIN_RANGE_BS_EVIDENCE_FILE;

	/**
	 * SQL queries
	 */
	public static final String GET_REFINED_MAPPINGS_SQL = "select DBP_SUB, DBP_OBJ from OIE_REFINED where OIE_SUB=? and OIE_PRED=? and OIE_OBJ=?";

	/**
	 * SQL for getting the top-1 subject and object mapping together.
	 */
	public static final String GET_TOP_1_TOGETHER_SQL = "select A.URI, I from (select  URI,  'S' as I, "
			+ "(SUM(COUNT)/(select  SUM(COUNT) from wikiPrep  where SF =?)) as p from wikiPrep  where "
			+ "SF =? group by BINARY URI order by p desc limit 1) as A UNION select B.URI, I from (select  URI, "
			+ " 'O' as I, (SUM(COUNT)/(select  SUM(COUNT) from wikiPrep  where SF =?)) as p from wikiPrep  "
			+ "where SF =? group by BINARY URI order by p desc limit 1) as B";

	/**
	 * SQL to fetch the probabilities of the same as links from terms to
	 * concepts
	 */
	public static final String GET_WIKI_LINKS_APRIORI_SQL = "select  URI, (SUM(COUNT)/(select  SUM(COUNT) from wikiPrep  where SF =?)) as p from wikiPrep  where SF =? group by BINARY URI order by p desc limit ?";

	/**
	 * find those properites which are actually mapped on both sub and obj
	 */
	public static final String GET_FULLY_MAPPED_OIE_PROPS_SQL = "select distinct OIE_PRED from OIE_REFINED where DBP_SUB <> 'X' and DBP_OBJ <> 'X'";

	public static final String GET_DOMAINS = "select distinct  d.INSTANCE_TYPE from OIE_REFINED n JOIN DBPEDIA_TYPES d ON n.DBP_SUB=d.DBPEDIA_INSTANCE where OIE_PRED =?";

	public static final String GET_RANGES = "select distinct  d.INSTANCE_TYPE from OIE_REFINED n JOIN DBPEDIA_TYPES d ON n.DBP_OBJ=d.DBPEDIA_INSTANCE where OIE_PRED =?";

	
	/**
	 * given a surface form, fetch top titles it refers to
	 */
	public static final String GET_WIKI_TITLES_SQL = "select URI, SUM(COUNT) as cnt from wikiPrep where SF = ? group by BINARY URI order by cnt desc limit ?";

	public static final String OIE_POSTFIXED = "INSERT INTO OIE_REFINED (OIE_SUB, OIE_PRED, OIE_OBJ, OIE_PFX_SUB, OIE_PFX_OBJ, DBP_SUB, DBP_OBJ) VALUES (?, ?, ?, ?, ?, ?, ?);";

	public static final String GET_DBPTYPE = "select INSTANCE_TYPE from DBPEDIA_TYPES where DBPEDIA_INSTANCE=?";

	public static final String UPDT_OIE_POSTFIXED = "UPDATE OIE_REFINED SET DBP_SUB=?, DBP_OBJ=? WHERE OIE_PFX_SUB=? AND OIE_PFX_OBJ=? AND OIE_PRED=?";

	public static final String GET_REFINED_FACT = "select DBP_SUB, DBP_OBJ from OIE_REFINED where OIE_SUB=? and OIE_PRED=? and OIE_OBJ=?";

	public static final String GET_OIE_PROPERTIES_ANNOTATED = "select distinct PHRASE, KB_PROP, EVAL, INV from OIE_PROP_GS where EVAL <> 'N' and EVAL <> ''";

	public static final String GET_KB_RELATIONS_ANNOTATED = "select KB_PROP, EVAL from OIE_PROP_GS where PHRASE = ? and EVAL <> '' and EVAL <> 'N'";

	public static final String QUERY_OBJECTTYPE = "select distinct ?val where {?val <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>} ";

	public static String WORDNET_API = null;

	public static String OIE_DATA_SEPERARTOR = null;

	/**
	 * insert DBPedia types SQL
	 */
	public static String INSERT_DBP_TYPES = "INSERT IGNORE INTO DBPEDIA_TYPES (DBPEDIA_INSTANCE, INSTANCE_TYPE) VALUES ( ?, ? )";

	/*
	 * DB Details
	 */

	// DB Driver name
	public static String DRIVER_NAME = "com.mysql.jdbc.Driver";

	// Url to conenct to the Database
	// public static String CONNECTION_URL = "jdbc:mysql://134.155.86.39/";
	public static String CONNECTION_URL = "jdbc:mysql://134.155.95.117:3306/";

	// name of the database
	public static String DB_NAME = "wikiStat";

	// user of the database. Make sure this user is created for the DB
	public static String DB_USER = "root";

	// password for the user
	public static String DB_PWD = "mannheim1234";

	public static Set<String> SUB_SET_TYPES = new HashSet<String>();

	public static Set<String> OBJ_SET_TYPES = new HashSet<String>();

	public static int HTTP_CONN_MAX_TOTAL = 0;

	public static int HTTP_CONN_MAX_TOTAL_PER_ROUTE = 0;

	public static int OPTI_INFLATION = 15;

	public static double OPTI_BETA = 0.5;

	/**
	 * load the variables from Configuration file
	 * 
	 * @param args
	 */
	public static void loadConfigParameters(String[] args) {

		Properties prop = new Properties();

		try {
			PREDICATE = args[0];

			// load a properties file
			prop.load(new FileInputStream(args[1]));

			PROPGTN_FACTOR = Double.parseDouble(prop
					.getProperty("TREE_PROPAGATION_FACTOR"));
			TOP_K_MATCHES = Integer.parseInt(prop.getProperty("TOPK_ANCHORS"));

			DBPEDIA_SPARQL_ENDPOINT = prop
					.getProperty("DBPEDIA_SPARQL_ENDPOINT");
			DBPEDIA_SPARQL_ENDPOINT_LOCAL = prop
					.getProperty("DBPEDIA_SPARQL_ENDPOINT_LOCAL");
			DBPEDIA_SPARQL_ENDPOINT_LIVE_DBP = prop
					.getProperty("DBPEDIA_SPARQL_ENDPOINT_LIVE_DBP");

			USE_LOGIT = Boolean.valueOf(prop.getProperty("USE_LOGIT"));
			IS_NELL = Boolean.valueOf(prop.getProperty("IS_NELL"));

			INCLUDE_YAGO_TYPES = Boolean.valueOf(prop
					.getProperty("INCLUDE_YAGO_TYPES"));

			RELOAD_TYPE = false;// Boolean.valueOf(prop.getProperty("RELOAD_TYPE"));
			Boolean.valueOf(prop.getProperty("LOAD_TYPES"));

			BATCH_SIZE = Integer.parseInt(prop.getProperty("BATCH_SIZE"));

			SCALE_WEIGHT = Integer.parseInt(prop.getProperty("SCALE_WEIGHT"));

			ENGAGE_INTER_STEP = Boolean.valueOf(prop
					.getProperty("ENGAGE_INTER_STEP"));

			OPTIMAL_INFLATION = Integer.parseInt(prop
					.getProperty("OPTIMAL_INFLATION"));

			TOP_K_NUMERIC_PROPERTIES = Integer.parseInt(prop
					.getProperty("TOP_K_NUMERIC_PROPERTIES"));

			OIE_DATA_PATH = prop.getProperty("OIE_DATA_PATH");

			OIE_DATA_SEPERARTOR = prop.getProperty("OIE_DATA_SEPERARTOR");

			// WORKFLOW_NORMAL = Boolean.valueOf(prop
			// .getProperty("WORKFLOW_NORMAL"));

			DBPEDIA_TBOX = prop.getProperty("DBPEDIA_TBOX");

			INSTANCE_THRESHOLD = prop.getProperty("INSTANCE_THRESHOLD");

			prop.getProperty("WORDNET_DICTIONARY");

			SIMILARITY_FACTOR = Double.parseDouble(prop
					.getProperty("SIMILARITY_FACTOR"));

			THREAD_MAX_POOL_SIZE = Integer.parseInt(prop
					.getProperty("THREAD_MAX_POOL_SIZE"));

			HTTP_CONN_MAX_TOTAL = Integer.parseInt(prop
					.getProperty("HTTP_CONN_MAX_TOTAL"));

			HTTP_CONN_MAX_TOTAL_PER_ROUTE = Integer.parseInt(prop
					.getProperty("HTTP_CONN_MAX_TOTAL_PER_ROUTE"));

			WORKFLOW = Integer.parseInt(prop.getProperty("WORKFLOW"));

			WORDNET_API = prop.getProperty("WORDNET_API");

			TIMEOUT_MINS = Integer.parseInt(prop.getProperty("TIMEOUT_MINS"));

			OPTI_BETA = Double.parseDouble(prop.getProperty("OPTI_BETA"));

			OPTI_INFLATION = Integer.parseInt(prop
					.getProperty("OPTI_INFLATION"));

			init();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	private static void init() {
		ALL_MLN_EVIDENCE = sample_dumps + PREDICATE + "/AllEvidence.db";

		BASIC_REASON_OUT_FILE = sample_dumps + PREDICATE + "/out.db";

		DOMAIN_RANGE_PREFERENCE_FILE = sample_dumps + PREDICATE
				+ "/domRanAlpha" + PROPGTN_FACTOR + "."
				+ String.valueOf(USE_LOGIT) + ".out";

		DOMAIN_RANGE_EVIDENCE_FILE = sample_dumps + PREDICATE
				+ "/domRanEvidence.db";

		DOMAIN_RANGE_BS_PREFERENCE_FILE = sample_dumps + PREDICATE
				+ "/domRanAlphaBS" + PROPGTN_FACTOR + "."
				+ String.valueOf(USE_LOGIT) + ".out";

		DOMAIN_RANGE_BS_EVIDENCE_FILE = sample_dumps + PREDICATE
				+ "/domRanEvidenceBS.db";

	}

}
