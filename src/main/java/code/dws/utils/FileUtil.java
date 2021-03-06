package code.dws.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to perform IO operations
 * 
 * @author Arnab Dutta
 */
public class FileUtil {

	private final static Logger logger = LoggerFactory
			.getLogger(FileUtil.class);

	public static ArrayList<ArrayList<String>> genericFileReader(
			InputStream inputStream, String valueSeperator, boolean hasHeader) {

		Scanner scan;
		scan = new Scanner(inputStream, "UTF-8");

		if (hasHeader) {
			scan.nextLine();
		}
		ArrayList<ArrayList<String>> lines = new ArrayList<ArrayList<String>>();

		while (scan.hasNextLine()) {

			ArrayList<String> tokens = new ArrayList<String>();

			String line = scan.nextLine();
			StringTokenizer st = new StringTokenizer(line, valueSeperator);

			while (st.hasMoreTokens()) {
				tokens.add(st.nextToken());
			}
			lines.add(tokens);
		}

		scan.close();

		return lines;
	}

	/**
	 * a file dump routine
	 * 
	 * @param properties
	 * @param fileName
	 */
	public static void writeOut(List<?> properties, String fileName) {
		BufferedWriter writer = null;
		String directory = new File(Constants.OIE_DATA_PATH).getParent();
		try {
			writer = new BufferedWriter(new FileWriter(directory + "/"
					+ fileName));
			for (Object elem : properties) {
				writer.write(elem.toString() + "\n");
			}
		} catch (IOException e) {
			logger.error("Something went wrong when writing out " + fileName
					+ "\n");
			e.printStackTrace();
		} finally {
			try {
				logger.info("Done writing to " + directory + "/" + fileName);
				writer.flush();
				if (writer != null)
					writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * read the file containing pairs of realtion, needed for computing
	 * similarities
	 * 
	 * @param string
	 * @param delimit
	 * @return
	 */
	public static List<Pair<String, String>> readPairsOfProperties(
			String string, String delimit) {
		String line = null;
		String[] arr = null;

		List<Pair<String, String>> retList = new ArrayList<Pair<String, String>>();

		try {
			@SuppressWarnings("resource")
			Scanner scan = new Scanner(new File(string));

			while (scan.hasNextLine()) {
				line = scan.nextLine();
				arr = line.split(delimit);

				retList.add(new ImmutablePair<String, String>(arr[0], arr[1]));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return retList;
	}
}
