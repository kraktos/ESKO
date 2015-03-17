/**
 * 
 */
package code.dws.ml;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.setup.Generator;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.QuerySolution;

/**
 * Class for estimating kernel densities. Used for detecting incorrect instance matches
 * 
 * @author arnab
 */
public class DensityEstimator
{

    /**
     * logger
     */
    public final static Logger logger = LoggerFactory.getLogger(DensityEstimator.class);

    static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");

    static SimpleDateFormat formateYear = new SimpleDateFormat("yyyy");

    static Map<String, Map<Pair<String, String>, Long>> GLBL_COLL =
        new HashMap<String, Map<Pair<String, String>, Long>>();

    static Map<String, Long> propMap = new HashMap<String, Long>();

    /**
     * @param args
     * @throws ParseException
     */
    public static void main(String[] args) throws ParseException
    {
        List<String> oieTriples = null;

        if (args.length != 1) {
            logger
                .error("Usage: java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.ml.DensityEstimator CONFIG.cfg");
        } else {
            Constants.loadConfigParameters(new String[] {"", args[0]});

            // load the reverb properties
            try {
                oieTriples = FileUtils.readLines(new File(Constants.OIE_DATA_PATH), "UTF-8");

            } catch (IOException e) {
                logger.error("Problem while reaing input OIE data file");
            }

            if (oieTriples != null) {
                logger.info("Loaded " + oieTriples.size() + " triples");

                parseTheTriples(oieTriples);
            }
        }
    }

    /**
     * build a distribution from the individual mappings
     * 
     * @param oieTriples
     * @throws ParseException
     */
    private static void parseTheTriples(List<String> oieTriples) throws ParseException
    {
        String[] elem = null;
        String oieSub = null;
        String oieRel = null;
        String oieObj = null;

        String kbSub = null;
        String kbObj = null;
        String sideProperty = null;

        double subVal = 0;
        double objVal = 0;

        List<String> subCands;
        List<String> objCands;
        List<String> domainSideProps;
        List<String> rangeSideProps;

        // init DB
        DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

        getValidProps();

        for (String line : oieTriples) {
            elem = line.split(Constants.OIE_DATA_SEPERARTOR);
            oieSub = elem[0];
            oieRel = elem[1];
            oieObj = elem[2];

            // process only the properties valid for the workflow
            if (propMap.containsKey(oieRel)) {
                // get the top-k concepts, confidence pairs
                // UTF-8 at this stage
                subCands = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities.cleanse(oieSub).replaceAll("\\_+", " "), 1);
                objCands = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities.cleanse(oieObj).replaceAll("\\_+", " "), 1);
                if (subCands != null && subCands.size() > 0)
                    kbSub = subCands.get(0).split("\t")[0];
                if (objCands != null && objCands.size() > 0)
                    kbObj = objCands.get(0).split("\t")[0];
                if (kbSub != null && kbObj != null) {
                    // logger.info(oieSub + "\t" + kbSub + "\t" + oieRel + "\t" + oieObj + "\t" + kbObj);
                    domainSideProps = createDistributionOnSideProperties(Utilities.utf8ToCharacter(kbSub));
                    rangeSideProps = createDistributionOnSideProperties(Utilities.utf8ToCharacter(kbObj));
                    if (domainSideProps.size() > 0 && rangeSideProps.size() > 0) {
                        // logger.info(domainSideProps + " <== " + oieRel + " ==> " + rangeSideProps);
                        checkCompatibility(domainSideProps, rangeSideProps, oieRel);
                    }
                }
            }

            //
            // sideProperty = "birthDate";
            // subVal = getNumericValue(sideProperty, kbSub);
            // objVal = getNumericValue(sideProperty, kbObj);
            //
            // if (subVal > 0 || objVal > 0)
            // logger.info("" + subVal + "\t" + objVal);
        }
    }

    private static void getValidProps()
    {
        List<String> props = Generator.getReverbProperties(-1, Long.parseLong(Constants.INSTANCE_THRESHOLD));

        for (String prop : props) {
            propMap.put(prop, 1L);
        }
    }

    private static void checkCompatibility(List<String> domainSideProps, List<String> rangeSideProps, String oieRel)
    {
        logger.info(oieRel);
        for (String domSideProp : domainSideProps) {
            for (String ranSideProp : rangeSideProps) {
                if (canBeCompared(domSideProp, ranSideProp)) {
                    logger.info("Comparing " + domSideProp + ", " + ranSideProp);
                    updateCollection(oieRel, new ImmutablePair<String, String>(domSideProp, ranSideProp));
                }
            }
        }
    }

    /**
     * @param oieRel
     * @param immutablePair
     */
    private static void updateCollection(String oieRel, ImmutablePair<String, String> immutablePair)
    {
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

    /**
     * @param domSideProp
     * @param ranSideProp
     * @return
     */
    private static boolean canBeCompared(String domSideProp, String ranSideProp)
    {
        String domType = null;
        String ranType = null;

        String query =
            "select distinct ?val where {<" + domSideProp + "> <http://www.w3.org/2000/01/rdf-schema#range> ?val}";

        List<QuerySolution> list = SPARQLEndPointQueryAPI.queryDBPediaEndPoint(query);
        if (list != null && list.size() > 0) {
            for (QuerySolution querySol : list) {
                domType = querySol.get("val").toString();
            }
        }

        query = "select distinct ?val where {<" + ranSideProp + "> <http://www.w3.org/2000/01/rdf-schema#range> ?val}";

        list = SPARQLEndPointQueryAPI.queryDBPediaEndPoint(query);
        if (list != null && list.size() > 0) {
            for (QuerySolution querySol : list) {
                ranType = querySol.get("val").toString();
            }
        }

        if (domType.equals(ranType) && domType.equals("http://www.w3.org/2001/XMLSchema#date"))
            return true;

        return false;
    }

    /**
     * retrieve the list of numerical properties for a given instance. These eventually form the side properties for a
     * given relation
     * 
     * @param kbInst
     * @return
     */
    private static List<String> createDistributionOnSideProperties(String kbInst)
    {
        String sideProp = null;
        List<String> sideProps = new ArrayList<String>();

        String query =
            "select distinct ?sideProp where {?kbInst ?sideProp ?b. ?sideProp <http://www.w3.org/2000/01/rdf-schema#range> ?dataType. "
                + "FILTER regex(str(?dataType), \"XMLSchema\"). "
                + "FILTER(?dataType != <http://www.w3.org/2001/XMLSchema#string>). "
                + "FILTER (!regex(str(?sideProp), \"wiki\", \"i\"))}";

        // logger.info(query);
        ParameterizedSparqlString sidePropQuery = new ParameterizedSparqlString(query);

        sidePropQuery.setIri("?kbInst", "http://dbpedia.org/resource/" + kbInst);
        // logger.info(sidePropQuery.toString());

        List<QuerySolution> list = SPARQLEndPointQueryAPI.queryDBPediaEndPoint(sidePropQuery.toString());
        if (list != null && list.size() > 0) {
            for (QuerySolution querySol : list) {
                sideProp = querySol.get("sideProp").toString();
                sideProps.add(sideProp);
            }
        }

        return sideProps;
    }

    /**
     * @param kbSub
     * @param sideProperty
     * @return
     * @throws ParseException
     */
    private static double getNumericValue(String sideProperty, String kbInst)
    {
        String dates = null;
        Date date = null;
        String year = null;

        List<QuerySolution> list =
            SPARQLEndPointQueryAPI.queryDBPediaEndPoint("select ?val where {<http://dbpedia.org/resource/" + kbInst
                + "> <http://dbpedia.org/ontology/" + sideProperty + "> ?val}");
        if (list.size() == 0)
            list =
                SPARQLEndPointQueryAPI.queryDBPediaEndPoint("select ?val where {<http://dbpedia.org/resource/" + kbInst
                    + "> <http://dbpedia.org/property/" + sideProperty + "> ?val}");

        for (QuerySolution querySol : list) {
            // Get the next result row
            // QuerySolution querySol = results.next();
            dates = querySol.get("val").toString();
            dates = StringUtils.substringBefore(dates, "^");

            try {
                date = formatDate.parse(dates);
                year = formateYear.format(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return (year != null) ? Double.valueOf(year) : 0;
    }
}
