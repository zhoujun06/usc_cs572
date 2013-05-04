import java.io.*;
import java.util.*;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.*;
import java.sql.*;
//import java.sql.ResultSetMetaData;

import org.apache.tika.*;
import org.apache.tika.io.*;
import org.apache.tika.parser.*;
import org.apache.tika.sax.*;
import org.apache.tika.metadata.*;
import org.apache.tika.parser.pdf.*;
import org.xml.sax.SAXException;
import org.apache.tika.exception.*;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;

import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.common.SolrInputDocument;
/*
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.CharTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.shingle.*;
import org.apache.lucene.analysis.shingle.ShingleFilter;
*/

public class GeoName { 
	PrintWriter logfile;
	Date timestamp;
	Connection conn;
	HashMap<String, HashMap<String, String>> geo_tag;
	HashSet<String> stopwords;
	HashSet<String> stopgram;
	String patn;
	String punc;
	SolrServer solr;

	public GeoName() {
		timestamp = new Date();
		geo_tag = new HashMap<String, HashMap<String, String>>();
		stopwords = new HashSet<String>();
		stopgram = new HashSet<String>();
		punc = "(\\*|%|\\+|=|!|;|:|,|\\?|\'|\"|\\.|\\s|&|#|-|_|<|>|~|/|@|\\$|\\(|\\)|\\d)";
		String urlString = "http://localhost:8080/solr-example/"; 
		solr = new HttpSolrServer(urlString);
		try {
			logfile = new PrintWriter("log.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void main (String [] argv) {
		GeoName ins = new GeoName();	
		ins.run();
	}

	public void run () {
		//String PDF_DIR = "./data/vault";
		String PDF_DIR = "./data/test";

		//try sql connection
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://127.0.0.1:3306/geodata";
		String user = "root";
		String passwd = "mysql";
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, passwd);
			conn.setAutoCommit(true);
		} catch (Exception e) {
			System.out.println("connect mysql error");
			e.printStackTrace();
			return ;
		}

		try {
			BufferedReader stopword_reader = new BufferedReader(new FileReader("stopwords.txt"));
			String str;
			while ((str = stopword_reader.readLine()) != null) {
				//str = str.toLowerCase();
				stopwords.add(str.toLowerCase());
			}
			stopword_reader.close();

			BufferedReader stopgram_reader = new BufferedReader(new FileReader("stopgram.txt"));
			while ((str = stopgram_reader.readLine()) != null) {
				//str = str.toLowerCase();
				stopgram.add(str.toLowerCase());
				System.out.println("stopgram: " + str.toLowerCase());
			}
			stopgram_reader.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

/*
		patn = "(";
		patn += stopwords.get(0);
		for (int i=1; i<stopwords.size(); i++) {
			patn += "|";
			patn += stopwords.get(i);
		}
		patn += ")";
		System.out.println(patn);
		*/

		File pdfdir = new File(PDF_DIR);
		File[] pdfs = pdfdir.listFiles(new PDFFilenameFilter());
		for (File pdf:pdfs) {
			processfile(pdf);
		}
		//System.out.println("overall: " + geo_tag);
	}

	public void processfile(File f) {
		
		//1 extract PDF file into a string
		//2 generate bigrams from the string
		//3 find the most frequent bigram
		//4 check whether the bigram is an address, from SQL match against
		//5 generate the hashmap from file to address of longtitude, latitude, bigram

		Parser parser = new PDFParser();
		BodyContentHandler handler = new BodyContentHandler(100*1024*1024);
		Metadata metadata = new Metadata();
		//metadata.set("org.apache.tika.parser.pdf.sortbyposition", "false");
		ParseContext context = new ParseContext();

		InputStream stream = null;
		try {
			stream = TikaInputStream.get(f);
			parser.parse(stream, handler, metadata, context);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				stream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		String doc_org = handler.toString();
		String doc = doc_org.replaceAll(punc, " ");
		//doc = doc.replaceAll(patn, "");

		//TODO add stop words
		////////////////////////////////////////////////////
		//if bigrams not found, then just use one gram
		boolean found = false;
		for (int ng = 2; ng > 0; ng--) {
			List<String> bigrams = ngrams(ng, doc);

			HashMap<String, Integer> map = new HashMap<String, Integer>();
			map.clear();
			ValCompare comp = new ValCompare(map);
			TreeMap<String, Integer> sort_map = new TreeMap<String, Integer>(comp);

			for (String gram:bigrams) {
				if (map.get(gram) != null) {
					int val = map.get(gram) + 1;
					map.put(gram, val);
					//System.out.println("Generate bigram: " + gram + ", freq " + val);
				} else {
					map.put(gram, 1);
					//System.out.println("Generate bigram: " + gram + ", freq 1");
				}
			}

			sort_map.putAll(map);

			String query = "";
			int ct = 0;
			query = "SELECT name,latitude,longitude from geonames WHERE MATCH(name, ansiname, alternatenames) AGAINST (? IN BOOLEAN MODE) limit 1";
			PreparedStatement stmt = null;
			ResultSet rs = null;

			Iterator it = sort_map.entrySet().iterator();
			HashMap<String, String> values = new HashMap<String, String>();
			try {
				stmt = conn.prepareStatement(query);

				while(it.hasNext()) {
					Map.Entry pair = (Map.Entry)it.next();
					String ky = (String) pair.getKey();

					if (stopgram.contains(ky.toLowerCase())) {
						System.out.println("get stopgram: " + ky);
						continue;
					}

					updatelog("gram: " + pair.getKey()+ ", val: " + pair.getValue());

					stmt.setString(1, "\""+(String)pair.getKey()+"\"");
					rs = stmt.executeQuery();
					if (rs.next()) {
						ResultSetMetaData md = rs.getMetaData();
						int col = md.getColumnCount();
						for (int i = 1; i<= col; i++) {
							String col_name = md.getColumnName(i);
							String col_value = rs.getString(col_name);
							values.put(col_name, col_value);
						}

						//geo_tag.put(f.getName(), values);
						found = true;
						break;
					}
				}

				if (found) {
					SolrInputDocument sdoc = new SolrInputDocument(); 
					sdoc.addField("id", f.getName());
					sdoc.addField("text", doc_org);
					sdoc.addField("address", values.get("name"));
					sdoc.addField("longitude", values.get("longitude"));
					sdoc.addField("latitude", values.get("latitude"));
					solr.add(sdoc);
					solr.commit();
					updatelog("SUCCESS " + f.getName() + values);

					break;
				}


			/*
				ContentStreamUpdateRequest up 
					= new ContentStreamUpdateRequest("/update/extract");

				up.addFile(f);

				up.setParam("literal.id", f.getName());
				up.setParam("uprefix", "attr_");
				//up.setParam("fmap.content", "attr_content");
				up.setParam("fmap.content", "text");
				up.setParam("literal.longitude", values.get("longitude"));
				up.setParam("literal.latitude", values.get("latitude"));
				up.setParam("literal.geoinfo", values.toString());

				up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);

				solr.request(up);
	*/

				//QueryResponse rsp = solr.query(new SolrQuery("jun zhou"));

				//System.out.println(rsp);

			} catch (Exception e) {
				e.printStackTrace();
				updatelog("ERROR: process " + f.getName() + " failed");
				updatelog("ERRORinfo" + values.toString());
			} finally {
				try {
					if (rs != null) rs.close();
					if (stmt != null) stmt.clearParameters();
				} catch (Exception e ) {
					e.printStackTrace();
				}
			}
		}

		if (!found) {
			updatelog("ERROR " + f.getName() + ": Not found geo info");
		} 
	}

	public List<String> ngrams(int n, String str) {
		List<String> ngrams = new ArrayList<String>();
		String[] words = str.split("\\s+");
		for (int i = 0; i < words.length - n + 1; i++) {
			boolean invalid = false;
			if (words[i].length() < 2) {
				continue;
			}
			for (int j=0; j<words[i].length(); j++) {
				if (words[i].charAt(j)!= ' ' && !Character.isLetter(words[i].charAt(j))) {
					invalid = true;
					break;
				}
			}
			if (invalid) {
				continue;
			}
			
			for (int j=0; j<n; j++) {
				if (stopwords.contains(words[i+j].toLowerCase())) {
					invalid = true;
					break;
				}
			}
			if (invalid) {
				continue;
			}

			ngrams.add(concat(words, i, i+n));
		}
		return ngrams;
	}

	public String concat(String[] words, int start, int end) {
		StringBuilder sb = new StringBuilder();
		for (int i = start; i < end; i++)
			sb.append((i > start ? " " : "") + words[i]);
		return sb.toString();
	}

	static class PDFFilenameFilter implements FilenameFilter {
		private Pattern p = Pattern.compile(".*\\.pdf",Pattern.CASE_INSENSITIVE);
		public boolean accept(File dir, String name) {
			Matcher m = p.matcher(name);
			return m.matches();
		}
	}

	private void updatelog(String info) {
		timestamp.setTime(System.currentTimeMillis());
		logfile.println(timestamp + " --: " + info);
		logfile.flush();
	}

	static class ValCompare implements Comparator<String> {
		Map<String, Integer> base;
		public ValCompare(Map<String, Integer> base) {
			this.base = base;
		}

		public int compare(String a, String b) {
			if (base.get(a) >= base.get(b)) {
				return -1;
			} else {
				return 1;
			}
		}
	}
}
