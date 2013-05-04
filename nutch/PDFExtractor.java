import java.io.*;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.regex.*;
import java.util.Arrays;
import java.util.zip.InflaterInputStream;
import java.net.URLDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchConfiguration;

public class PDFExtractor {
	
	File in_dir;
	File out_dir;
	int url_num;
	int pdf_num;
	PrintWriter log_file;
	//String pdf_p_str = "([^/]*pdf)";
	String pdf_p_str = "/([^/]+)/at_download/file";
	Pattern pdf_p;
	Date timestamp;

	public PDFExtractor(String in, String out) {
		url_num = 0;
		pdf_num = 0;
		timestamp = new Date();
		pdf_p = Pattern.compile(pdf_p_str, Pattern.CASE_INSENSITIVE);

		try {
			in_dir = new File(in);
			out_dir = new File(out);

			if (!in_dir.exists()) {
				System.out.println("Error: input path not exist");
				System.exit(1);
			}

			if (!out_dir.exists()) {
				boolean res = out_dir.mkdir();
				if (!res) {
					System.out.println("Error: can't create output directory");
					System.exit(1);
				}
			}

			log_file = new PrintWriter("log.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	protected void finalize() throws Throwable {
		try {
			log_file.close();
		} finally {
			super.finalize();
		}
	}

	public static void main(String[] args) {
		String usage = "Usage: java -cp crawl_data_path output_path";
		System.out.println("Input path is: " + args[0]);
		System.out.println("Output path is: " + args[1]);
		if (args.length != 2) {
			System.out.println(usage);
			return;
		}

		PDFExtractor ins = new PDFExtractor(args[0], args[1]);
		ins.run();
	}

	/**
	 * open the input folder, iterate through each part
	 * check the url pattern
	 */
	private void run() {

		try {
			File[] parts = in_dir.listFiles();

			for (File part:parts) {
				process(part);
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		//print output stats
		System.out.println("number of urls crawled: " + url_num);
		System.out.println("number of pdf files crawled: " + pdf_num);
	}


	private void process(File seg) {
		//open the real data file
		try {
			Path file = new Path(seg.getPath(), "content/part-00000/data");
			Configuration conf = NutchConfiguration.create();
			FileSystem fs = FileSystem.get(conf);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

			Text key = new Text();
			Content content = new Content();

			while (reader.next(key, content)) {
				update_log(key.toString());
				url_num++;

				Matcher m = pdf_p.matcher(key.toString());
				if (m.find()) {
					String name = m.group(1);
					name = URLDecoder.decode(name, "UTF-8");
					name = name + ".pdf";
					Path out_p = new Path(out_dir.getPath(), name);
					OutputStream fout = new FileOutputStream(out_p.toString());
					fout.write(content.getContent(), 0, content.getContent().length );
					update_log("\tpdf found: " + name);
					pdf_num++;
				}
			}
			reader.close();
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void update_log(String info) {
		timestamp.setTime(System.currentTimeMillis());
		log_file.println(timestamp + " -- " + info);
		log_file.flush();
	}
}
