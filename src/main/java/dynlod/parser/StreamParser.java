package dynlod.parser;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.junit.Test;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.rio.turtle.TurtleParser;

import dynlod.utils.Formats;

public class StreamParser {
	
	public StatementProcessor processor = new StatementProcessor();

	@Test
	public void Parse(String url, String format) {

		try {
//			java.net.URL documentUrl = new URL(
//					"http://localhost:9090/dataid/dataids_example/long_abstracts_eo.ttl");
			java.net.URL documentUrl = new URL(url);
			InputStream inputStream = documentUrl.openStream();

			RDFParser rdfParser = null;
			
			if(format.equals(Formats.DEFAULT_TURTLE))
				rdfParser = new TurtleParser();
			else if(format.equals(Formats.DEFAULT_NTRIPLES))
				rdfParser = new NTriplesParser();
			else if(format.equals(Formats.DEFAULT_RDFXML))
				rdfParser = new RDFXMLParser();

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
