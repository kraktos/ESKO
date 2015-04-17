package code.dws;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

import com.hp.hpl.jena.query.QuerySolution;

public class ttt {

	public static void main(String[] args) throws IOException {
		List<QuerySolution> list = null;
		String s;

		String[] values = {"Website", "RadioProgram", "Artwork", "Cartoon", "Film", 
				"Musical", "MusicalWork", "Software", "TelevisionShow", "TelevisionEpisode",
				"TelevisionSeason", "WrittenWork"};
		
		for (String val:values) {
			BufferedWriter scriptWrtr = new BufferedWriter(new FileWriter(
					"/home/adutta/git/DATA/" + val + ".dat"));
			Constants.loadConfigParameters(new String[] { "", "CONFIG.cfg" });
			String query = "select * where {?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
					+ " <http://dbpedia.org/ontology/" + val + ">}";
			for (int k = 0; k < 200000; k++) {
				s = query + " limit 1000 offset " + k;
				k = k + 1000;

				System.out.println(s);
				list = SPARQLEndPointQueryAPI.queryDBPediaEndPoint(s);
				System.out.println(list.size());

				for (QuerySolution querySol : list) {
					scriptWrtr.write(Utilities.utf8ToCharacter(querySol
							.get("a").toString()) + "\n");
				}
				scriptWrtr.flush();
			}
			scriptWrtr.close();
		}

	}
}
