import java.io.*;
import java.util.*;
import java.util.regex.*;
import org.apache.tika.*;
import org.apache.tika.io.*;
import org.apache.tika.parser.*;
import org.apache.tika.sax.*;
import org.apache.tika.metadata.*;
import org.apache.tika.parser.pdf.*;
import org.xml.sax.SAXException;
import org.apache.tika.exception.*;

public class TikaHW {

	List<String> keywords;
	PrintWriter logfile;
	int num_keywords, num_files, num_fileswithkeywords;
	Map<String,Integer> keyword_counts;
	Date timestamp;

	/**
	 * constructor
	 * DO NOT MODIFY
	 */
	public TikaHW() {
		keywords = new ArrayList<String>();
		num_keywords=0;
		num_files=0;
		num_fileswithkeywords=0;
		keyword_counts = new HashMap<String,Integer>();
		timestamp = new Date();
		try {
			logfile = new PrintWriter("log.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * destructor
	 * DO NOT MODIFY
	 */
	protected void finalize() throws Throwable {
		try {
			logfile.close();
	    } finally {
	        super.finalize();
	    }
	}

	/**
	 * main() function
	 * instantiate class and execute
	 * DO NOT MODIFY
	 */
	public static void main(String[] args) {
		TikaHW instance = new TikaHW();
		instance.run();
	}

	/**
	 * execute the program
	 * DO NOT MODIFY
	 */
	private void run() {

		// Open input file and read keywords
		try {
			BufferedReader keyword_reader = new BufferedReader(new FileReader("keywords.txt"));
			String str;
			while ((str = keyword_reader.readLine()) != null) {
				//str = str.toLowerCase();
				keywords.add(str);
				num_keywords++;
				keyword_counts.put(str, 0);
			}
			keyword_reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Open all pdf files, process each one
		File pdfdir = new File("./vault");
		//File pdfdir = new File("./test");
		File[] pdfs = pdfdir.listFiles(new PDFFilenameFilter());
		for (File pdf:pdfs) {
			num_files++;
			processfile(pdf);
		}

		// Print output file
		try {
			PrintWriter outfile = new PrintWriter("output.txt");
			outfile.print("Keyword(s) used: ");
			if (num_keywords>0) outfile.print(keywords.get(0));
			for (int i=1; i<num_keywords; i++) outfile.print(", "+keywords.get(i));
			outfile.println();
			outfile.println("No of files processed: " + num_files);
			outfile.println("No of files containing keyword(s): " + num_fileswithkeywords);
			outfile.println();
			outfile.println("No of occurrences of each keyword:");
			outfile.println("----------------------------------");
			for (int i=0; i<num_keywords; i++) {
				String keyword = keywords.get(i);
				outfile.println("\t"+keyword+": "+keyword_counts.get(keyword));
			}
			outfile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Process a single file
	 * 
	 * Here, you need to:
	 *  - use Tika to extract text contents from the file
	 *  - (optional) check OCR quality before proceeding
	 *  - search the extracted text for the given keywords
	 *  - update num_fileswithkeywords and keyword_counts as needed
	 *  - update log file as needed
	 * 
	 * @param f File to be processed
	 */
	private void processfile(File f) {

		/***** YOUR CODE GOES HERE *****/

		Parser parser = new PDFParser();
		BodyContentHandler handler = new BodyContentHandler(10*1024*1024);
		Metadata metadata = new Metadata();
		//metadata.set("org.apache.tika.parser.pdf.sortbyposition", "false");
		ParseContext context = new ParseContext();

		InputStream stream = null;
		try {
			stream = TikaInputStream.get(f);
			parser.parse(stream, handler, metadata, context);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (TikaException e) {
			e.printStackTrace();
		} finally {
			try {
				stream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		//System.out.println(keywords.get(0));

		//Matcher m = p.matcher(handler.toString());

		//construct the regex pattern
		String punc = "(!|;|:|,|\\?|\'|\"|\\.|\\s)";
		String pre = punc + "(";
		String suf = ")" + punc;


		boolean found = false;
		for (int i=0; i < keywords.size(); i++) {
			String kws = pre + keywords.get(i) + suf;
			kws = kws.replace(" ", "\\s+");
			//System.out.println(kws);

			Pattern p = Pattern.compile(kws,Pattern.CASE_INSENSITIVE);
			//Pattern p = Pattern.compile(kws);
			Matcher m = p.matcher(handler.toString());

			while(m.find()) {
				//String kwd = m.group().toLowerCase(); 
				String kwd = m.group(2); 
				String keywd = keywords.get(i);
				if (kwd != null) {
					kwd = kwd.replace("\\s+", " ");
					try {
					int num = keyword_counts.get(keywd) + 1;
						keyword_counts.put(keywd, num);
					} catch (NullPointerException e) {
						e.printStackTrace();
						System.out.println("keyword is: "+kwd);
					}
					found = true;

					//updatelog(keywd, f.getName());
					//updatelog(kwd, f.getName());
					updatelog(m.group(), f.getName());
				}
			}
		}

		if(found) { 
			num_fileswithkeywords++;
		}

	}

	/**
	 * Update the log file with search hit
	 * Appends a log entry with the system timestamp, keyword found, and filename of PDF file containing the keyword
	 * DO NOT MODIFY
	 */
	private void updatelog(String keyword, String filename) {
		timestamp.setTime(System.currentTimeMillis());
		logfile.println(timestamp + " -- \"" + keyword + "\" found in file \"" + filename +"\"");
		logfile.flush();
	}

	/**
	 * Filename filter that accepts only *.pdf
	 * DO NOT MODIFY 
	 */
	static class PDFFilenameFilter implements FilenameFilter {
		private Pattern p = Pattern.compile(".*\\.pdf",Pattern.CASE_INSENSITIVE);
		public boolean accept(File dir, String name) {
			Matcher m = p.matcher(name);
			return m.matches();
		}
	}
}
