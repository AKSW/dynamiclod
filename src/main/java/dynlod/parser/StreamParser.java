package dynlod.parser;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;

import org.junit.Test;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.turtle.TurtleParser;

public class StreamParser {

	@Test
	public void Parse(String url, String format) {

		try {
			java.net.URL documentUrl = new URL(
					"http://localhost:9090/dataid/dataids_example/long_abstracts_eo.ttl");
			InputStream inputStream = documentUrl.openStream();

			RDFParser rdfParser = new TurtleParser();

			StatementProcessor processor = new StatementProcessor();
			rdfParser.setRDFHandler(processor);

			try {
				rdfParser.parse(inputStream, documentUrl.toString());
			} catch (IOException e) {
				// handle IO problems (e.g. the file could not be read)
			} catch (RDFParseException e) {
				// handle unrecoverable parse error
			} catch (RDFHandlerException e) {
				// handle a problem encountered by the RDFHandler
			}

			System.out.println(processor.getCountedStatements());

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
	}

}
