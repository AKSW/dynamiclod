package dynlod.parser;

import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.RDFHandlerBase;

public class StatementProcessor extends RDFHandlerBase {
	private int countedStatements = 0;

	@Override
	public void handleStatement(Statement st) {
		countedStatements++;
		System.out.println(st.getObject());
	}

	public int getCountedStatements() {
		return countedStatements;
	}
}
