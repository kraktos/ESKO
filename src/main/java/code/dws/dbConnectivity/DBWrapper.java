/**
 * 
 */

package code.dws.dbConnectivity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.dto.FactDao;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

/**
 * Wrapper class to initiate the DB operations. Used on top of
 * {@link DBConnection}
 * 
 * @author Arnab Dutta
 */
public class DBWrapper {

	// define Logger
	private static Logger logger = Logger.getLogger(DBWrapper.class.getName());

	// DB connection instance, one per servlet
	private static Connection connection = null;

	// DBCOnnection
	private static DBConnection dbConnection = null;

	// prepared statement instance
	private static PreparedStatement pstmt = null;

	private static PreparedStatement insertDBPTypePrepstmnt = null;

	private static PreparedStatement insertOIEPFxdPrepstmnt = null;

	private static PreparedStatement fetchDbpTypePrepstmnt = null;

	private static PreparedStatement fetchAnnoOIERelationsPrepstmnt = null;

	private static int batchCounter = 0;

	/**
	 * initiates the connection parameters
	 * 
	 * @param sql
	 */
	public static void init(String sql) {
		try {
			// instantiate the DB connection
			dbConnection = new DBConnection();

			// retrieve the freshly created connection instance
			connection = dbConnection.getConnection();

			// create a statement
			pstmt = connection.prepareStatement(sql);
			// for DBPedia types
			insertDBPTypePrepstmnt = connection
					.prepareStatement(Constants.INSERT_DBP_TYPES);
			insertOIEPFxdPrepstmnt = connection
					.prepareStatement(Constants.OIE_POSTFIXED);
			fetchDbpTypePrepstmnt = connection
					.prepareStatement(Constants.GET_DBPTYPE);

			fetchAnnoOIERelationsPrepstmnt = connection
					.prepareStatement(Constants.GET_KB_RELATIONS_ANNOTATED);

			connection.setAutoCommit(false);

		} catch (SQLException ex) {
			ex.printStackTrace();
			logger.error("Connection Failed! Check output console"
					+ ex.getMessage());
		}
	}

	public static void saveToOIEPostFxd(String oieSub, String oiePred,
			String oieObj, String oieSubPfxd, String oieObjPfxd) {

		try {

			insertOIEPFxdPrepstmnt.setString(1, oieSub);
			insertOIEPFxdPrepstmnt.setString(2, oiePred);
			insertOIEPFxdPrepstmnt.setString(3, oieObj);
			insertOIEPFxdPrepstmnt.setString(4, oieSubPfxd);
			insertOIEPFxdPrepstmnt.setString(5, oieObjPfxd);
			insertOIEPFxdPrepstmnt.setString(6, "X");
			insertOIEPFxdPrepstmnt.setString(7, "X");

			insertOIEPFxdPrepstmnt.addBatch();
			insertOIEPFxdPrepstmnt.clearParameters();

			batchCounter++;
			if (batchCounter % Constants.BATCH_SIZE == 0
					&& batchCounter > Constants.BATCH_SIZE) { // batches are
				// flushed at
				// a time
				// execute batch update
				insertOIEPFxdPrepstmnt.executeBatch();

				logger.info("FLUSHED TO OIE_REFINED...");

				connection.commit();
				insertOIEPFxdPrepstmnt.clearBatch();
			}

		} catch (SQLException e) {
			logger.error("Error with batch insertion of OIE_REFINED .."
					+ e.getMessage());
		}

	}

	/**
	 * find the KB relations for a given OIE relation
	 * 
	 * @param oieRelation
	 * @return
	 */
	public static List<Pair<String, String>> fetchKBRelationsForAnOIERelation(
			String oieRelation) {
		List<Pair<String, String>> types = new ArrayList<Pair<String, String>>();

		try {
			// if not cached
			// if (!EvidenceBuilder.INSTANCE_TYPES.containsKey(instance)) {

			fetchAnnoOIERelationsPrepstmnt.setString(1, oieRelation);
			ResultSet rs = fetchAnnoOIERelationsPrepstmnt.executeQuery();

			while (rs.next()) {
				types.add(new ImmutablePair<String, String>(rs.getString(1), rs
						.getString(2)));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return types;
	}

	public static List<String> getDBPInstanceType(String instance) {
		List<String> types = new ArrayList<String>();

		try {
			// if not cached
			// if (!EvidenceBuilder.INSTANCE_TYPES.containsKey(instance)) {

			fetchDbpTypePrepstmnt.setString(1, instance);
			ResultSet rs = fetchDbpTypePrepstmnt.executeQuery();

			while (rs.next()) {
				types.add(rs.getString(1));
			}
			// cache it
			// EvidenceBuilder.INSTANCE_TYPES.put(instance, types);
			// } else {
			// return EvidenceBuilder.INSTANCE_TYPES.get(instance);
			// }
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return types;
	}

	

	/**
	 * find the top k candidates for a given surface form/term/ oie instance
	 * 
	 * @param arg
	 * @param limit
	 * @return
	 */
	public static List<String> fetchTopKLinksWikiPrepProb(String arg, int limit) {
		ResultSet rs = null;
		List<String> results = null;
		List<String> temp = null;

		DecimalFormat decimalFormatter = new DecimalFormat("0.00000");

		if (arg == null || arg.length() == 0)
			return new ArrayList<String>();
		try {
			// if (!EvidenceBuilder.INSTANCE_CANDIDATES.containsKey(arg)) {

			pstmt.setString(1, arg.trim());
			pstmt.setString(2, arg.trim());
			pstmt.setInt(3, limit);

			rs = pstmt.executeQuery();
			results = new ArrayList<String>();
			temp = new ArrayList<String>();

			while (rs.next()) {

				results.add(Utilities.characterToUTF8((rs.getString(1))
						.replaceAll("\\s", "_"))
						+ "\t"
						+ decimalFormatter.format(rs.getDouble(2)));

				temp.add(Utilities.characterToUTF8((rs.getString(1))
						.replaceAll("\\s", "_"))
						+ "\t"
						+ decimalFormatter.format(rs.getDouble(2)));
			}

			// EvidenceBuilder.INSTANCE_CANDIDATES.put(arg, temp);
			// } else {
			// return EvidenceBuilder.INSTANCE_CANDIDATES.get(arg);
			// }
		} catch (Exception e) {

			logger.error(" exception while fetching " + arg + " "
					+ e.getMessage());
		}

		return results;
	}

	public static void saveResidualOIERefined() {
		try {
			if (batchCounter % Constants.BATCH_SIZE != 0) {
				insertOIEPFxdPrepstmnt.executeBatch();
				logger.info("FLUSHED TO OIE_REFINED...");
				connection.commit();
			}
		} catch (SQLException e) {
		}
	}

	public static void updateResidualOIERefined() {
		try {
			if (batchCounter % Constants.BATCH_SIZE != 0) {
				pstmt.executeBatch();
				logger.info("FLUSHED TO OIE_REFINED...");
				connection.commit();
			}
		} catch (SQLException e) {
		}
	}

	public static void updateOIEPostFxd(String oieSub, String oiePred,
			String oieObj, String dbpS, String dbpO) {

		try {

			pstmt.setString(1, dbpS);
			pstmt.setString(2, dbpO);

			pstmt.setString(3, oieSub);
			pstmt.setString(4, oieObj);
			pstmt.setString(5, oiePred);

			pstmt.addBatch();
			pstmt.clearParameters();

			batchCounter++;

			if (batchCounter % Constants.BATCH_SIZE == 0
					&& batchCounter > Constants.BATCH_SIZE) { // batches are
				// flushed at
				// a time
				// execute batch update
				pstmt.executeBatch();

				logger.info("FLUSHED TO OIE_REFINED");
				connection.commit();
				pstmt.clearBatch();
			}

		} catch (SQLException e) {
			logger.error("Error with batch update of OIE_REFINED .."
					+ e.getMessage());
		}

	}

	/**
	 * insert into the DBPEDIA_TYPES table, should run once
	 * 
	 * @param instance
	 * @param instType
	 */
	public static void saveToDBPediaTypes(String instance, String instType) {

		try {

			insertDBPTypePrepstmnt.setString(1, instance);
			insertDBPTypePrepstmnt.setString(2, instType);

			insertDBPTypePrepstmnt.addBatch();
			insertDBPTypePrepstmnt.clearParameters();

			batchCounter++;
			if (batchCounter % Constants.BATCH_SIZE == 0
					&& batchCounter > Constants.BATCH_SIZE) { // batches are
				// flushed at
				// a time
				// execute batch update
				insertDBPTypePrepstmnt.executeBatch();

				logger.info("FLUSHED TO DBPEDIA_TYPES");
				connection.commit();
				insertDBPTypePrepstmnt.clearBatch();
			}

		} catch (SQLException e) {
			logger.error("Error with batch insertion of DBPEDIA_TYPES .."
					+ e.getMessage());
		}

	}

	/**
	 * inserts residual rows after batch
	 */
	public static void saveResidualDBPTypes() {
		try {
			if (batchCounter % Constants.BATCH_SIZE != 0) {
				insertDBPTypePrepstmnt.executeBatch();
				logger.info("FLUSHED TO DBPEDIA_TYPES...");
				connection.commit();
			}
		} catch (SQLException e) {
		}
	}

	/**
	 * shutting down the DB, its connections and statements
	 */
	public static void shutDown() {

		if (pstmt != null) {
			try {
				pstmt.close();
			} catch (Exception excp) {
			}
		}

		if (fetchDbpTypePrepstmnt != null) {
			try {
				fetchDbpTypePrepstmnt.close();
			} catch (Exception excp) {
			}
		}

		if (fetchAnnoOIERelationsPrepstmnt != null) {
			try {
				fetchAnnoOIERelationsPrepstmnt.close();
			} catch (Exception excp) {
			}
		}

		if (insertDBPTypePrepstmnt != null) {
			try {
				insertDBPTypePrepstmnt.close();
			} catch (Exception excp) {
			}
		}

		if (insertOIEPFxdPrepstmnt != null) {
			try {
				insertOIEPFxdPrepstmnt.close();
			} catch (Exception excp) {
			}
		}

		dbConnection.shutDown();

	}

	/**
	 * retrieves the list of fully annotated property mappings
	 * 
	 * @return
	 */
	public static Map<String, List<List<String>>> getAnnoPairs() {

		Map<String, List<List<String>>> results = null;
		List<String> attributes = null;
		List<List<String>> val = null;

		ResultSet rs = null;

		String oiePred = null;

		try {
			rs = pstmt.executeQuery();
			results = new HashMap<String, List<List<String>>>();

			while (rs.next()) {
				attributes = new ArrayList<String>();
				oiePred = rs.getString(1);

				attributes.add(rs.getString(2));
				attributes.add(rs.getString(3));
				attributes.add(rs.getString(4));

				if (results.containsKey(oiePred)) {
					val = results.get(oiePred);
				} else {
					val = new ArrayList<List<String>>();
				}
				val.add(attributes);
				results.put(oiePred, val);
			}
		} catch (Exception e) {
		}
		return results;
	}

	/**
	 * method to retrieve the refined KB fact for a given fact
	 * 
	 * @param key
	 * @return
	 */
	public static FactDao getRefinedDBPFact(FactDao key) {

		String dbpSub = null;
		String dbpObj = null;

		try {

			pstmt.setString(1, key.getSub());
			pstmt.setString(2, key.getRelation());
			pstmt.setString(3, key.getObj());
			ResultSet rs = pstmt.executeQuery();

			if (rs.next()) {
				dbpSub = (rs.getString(1).equals("X")) ? "?" : rs.getString(1);
				dbpObj = (rs.getString(2).equals("X")) ? "?" : rs.getString(2);

				return new FactDao(Utilities.utf8ToCharacter(dbpSub), "?",
						Utilities.utf8ToCharacter(dbpObj));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * method to find the refined mappings for the oie subject and object from
	 * DB. This is the stage after IM is complete.
	 * 
	 * @param oieSub
	 * @param pred
	 * @param oieObj
	 * @return
	 */
	public static List<String> fetchRefinedMapping(String oieSub, String pred,
			String oieObj) {
		ResultSet rs = null;
		List<String> results = null;

		try {
			pstmt.setString(1, oieSub);
			pstmt.setString(2, pred);
			pstmt.setString(3, oieObj);

			rs = pstmt.executeQuery();
			results = new ArrayList<String>();

			while (rs.next()) {

				results.add(Utilities.characterToUTF8((Utilities
						.utf8ToCharacter(rs.getString(1)))
						.replaceAll("\\s", "_").replaceAll("\\[", "\\(")
						.replaceAll("\\]", "\\)")));
				results.add(Utilities.characterToUTF8((Utilities
						.utf8ToCharacter(rs.getString(2)))
						.replaceAll("\\s", "_").replaceAll("\\[", "\\(")
						.replaceAll("\\]", "\\")));
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return results;
	}

}
