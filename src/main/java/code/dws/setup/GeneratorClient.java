/**
 * 
 */
package code.dws.setup;

import java.util.List;

import code.dws.utils.Constants;
import code.dws.utils.FileUtil;

/**
 * Client for the {@link Generator}
 * 
 * @author adutta
 *
 */
public class GeneratorClient {

	/**
	 * 
	 */
	public GeneratorClient() {

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// load the configuration from the file
		Constants.loadConfigParameters(new String[] { "", args[0] });

		List<String> properties = Generator.getOIERelations(-1, 100L);

		FileUtil.writeOut(properties, "Top." + 100 + ".Reverb.Properties.dat");
	}
}
