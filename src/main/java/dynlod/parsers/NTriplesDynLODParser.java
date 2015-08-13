package dynlod.parsers;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.helpers.RDFParserBase;

public class NTriplesDynLODParser extends RDFParserBase{

//	RDFHandlerBase handlerClass;
	Queue<String> bufferQueue = new LinkedBlockingQueue<String>();
	boolean doneReading = false;
	
	public void stream(InputStream inStream){
		
		final InputStream inputStream = inStream;
		
		try {
		new Thread(
		        new Runnable() {
		            public void run () {
		                try {

		                	int nRead;
		                	byte[] data = new byte[655360];

		                	while ((nRead = new BufferedInputStream(inputStream).read(data, 0, data.length)) != -1) {
//		                	  out.write(data, 0, nRead)
		                		bufferQueue.add(new String(data, StandardCharsets.UTF_8));
		                		if(bufferQueue.size()>2000)
		                			Thread.sleep(1);
		                	}
		                	doneReading = true;
		                }
		                catch (IOException e) {
		                    // logging and exception handling should go here
		                } catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
		            }
		        }
		).start();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
	}
	
//	public void setHandler(RDFHandlerBase handlerClass){
//		this.handlerClass = handlerClass;
//	}
	
	protected void parse(){
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		Statement s = null;
		try {


			ValueFactory factory = ValueFactoryImpl.getInstance();
			URI SubjectStmt = null;
			URI PropertyStmt= null;
			URI ObjectStmt= null;
			

			String lastLine = "";
			String tmpLastSubject = "";

			// starts reading buffer queue
			while (!doneReading) {
				Thread.sleep(10);

				while (bufferQueue.size() > 0) {
					// aint.decrementAndGet();
					try {
						String o[];
						o = bufferQueue.remove().split("\n");
						if (!lastLine.equals("")) {
							o[0] = lastLine.concat(o[0]);

							lastLine = "";
						}

						for (int q = 0; q < o.length; q++) {
							String u = o[q];
//							System.out.println(u);
							if (!u.startsWith("#")) {
								try {

									Pattern pattern = Pattern
											.compile("^(<[^>]+>)\\s+(<[^>]+>)\\s(.*)(\\s\\.)");

									Matcher matcher = pattern.matcher(u);
									if (!matcher.matches()) {
										throw new ArrayIndexOutOfBoundsException();
									}
									if (!matcher
											.group(3)
											.equals("<http://www.w3.org/2002/07/owl#Class>")
											&& !matcher
													.group(2)
													.equals("<http://www.w3.org/2000/01/rdf-schema#subClassOf>")) {
//									if(true){

										// get subject and save to file
//										if (subject != null) {
											if (!tmpLastSubject.equals(matcher
													.group(1))) {
												tmpLastSubject = matcher
														.group(1);
//												subject.write(new String(
//														matcher.group(1) + "\n")
//														.getBytes());
												SubjectStmt = factory.createURI(new String(matcher.group(1).replace("<", "").replace(">","")));
												PropertyStmt = factory.createURI("http://www.a.com");
												
											}
//										}

										// get object (make sure that its a
										// resource and not a literal), add
										// to queue and save to file
//										if (object != null)
//											if (!matcher.group(3).startsWith(
//													"\"")) {
//												object.write(new String(matcher
//														.group(3) + "\n")
//														.getBytes());
												ObjectStmt = factory.createURI(new String(matcher.group(3).replace("<", "").replace(">","")));

												// add object to object queue
												// (the queue is read by other
												// thread)
//												while (objectQueue.size() > 1000) {
//													Thread.sleep(1);
//												}
//												if (isChain)
//													objectQueue.add(matcher
//															.group(3));
//												objectLines++;
											}
											Statement nameStatement = factory.createStatement(SubjectStmt, PropertyStmt, ObjectStmt);
//											handlerClass.handleStatement(nameStatement);
											getRDFHandler().handleStatement(nameStatement);
//									}

								} catch (ArrayIndexOutOfBoundsException e) {
									// e.printStackTrace();
									lastLine = u;
								}
							}
						}

					} catch (NoSuchElementException em) {
						// em.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			}

			

		} catch (Exception e) {
			e.printStackTrace();

		}
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
	}

	@Override
	public RDFFormat getRDFFormat() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void parse(InputStream in, String baseURI) throws IOException,
			RDFParseException, RDFHandlerException {
		stream(in);
		parse();
		
	}

	@Override
	public void parse(Reader reader, String baseURI) throws IOException,
			RDFParseException, RDFHandlerException {
		// TODO Auto-generated method stub
		
	}
	
	
}
