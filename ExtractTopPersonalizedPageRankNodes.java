package ca.uwaterloo.cs451.a4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import java.util.regex.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.IOUtils;
import tl.lin.data.pair.PairOfFloatInt;
import org.apache.hadoop.io.IntWritable;
import ca.uwaterloo.cs451.a4.PageRankNode;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import org.apache.hadoop.fs.FSDataOutputStream;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
	private static final String REGEX = "part-m-[0-9]{5}";
	private static final Pattern PATTERN = Pattern.compile(REGEX);
	private SequenceFile.Reader[] index;
	private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

	private static final String NEXT_LINE = "\n";

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
	}

	public ExtractTopPersonalizedPageRankNodes() {
	}

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String SOURCE_NODES = "sources";
	private static final String TOP = "top";

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access" })
	public int run(String[] args) throws Exception {
		Options options = new Options();

		// options.addOption(new Option(COMBINER, "use combiner"));
		// options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));
		// options.addOption(new Option(RANGE, "use range partitioner"));

		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path").create(INPUT));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(OUTPUT));
		options.addOption(
				OptionBuilder.withArgName("num").hasArg().withDescription("source nodes").create(SOURCE_NODES));
		options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("top").create(TOP));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			return -1;
		}

		if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(SOURCE_NODES)
				|| !cmdline.hasOption(TOP)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String inPath = cmdline.getOptionValue(INPUT);
		String outPath = cmdline.getOptionValue(OUTPUT);
		String sourceNodes = cmdline.getOptionValue(SOURCE_NODES);
		int top = Integer.parseInt(cmdline.getOptionValue(TOP));

		LOG.info("Tool name: ExtractTopPersonalizedPageRankNodes");
		LOG.info(" - input dir: " + inPath);
		LOG.info(" - output dir: " + outPath);
		LOG.info(" - sourceNodes: " + sourceNodes);
		LOG.info(" - top: " + top);

		String[] sourceNodeValues = sourceNodes.trim().split(",");

		FileSystem fs = FileSystem.get(new Configuration());

		readFromHDFS(inPath, fs, sourceNodeValues, top, outPath);
		return 0;
	}

	private void readFromHDFS(String inputPath, FileSystem fs, String[] sourceNodeValues, int top, String outPath)
			throws IOException {
		List<String> fileList = new ArrayList<String>();
		FileStatus[] fileStatus = fs.listStatus(new Path(inputPath));
		System.out.println("File ststus" + fileStatus.length);

		if (null != fileStatus) {
			for (FileStatus fileS : fileStatus) {
				String name = fileS.getPath().getName();
				Matcher matcher = PATTERN.matcher(name);
				if (matcher.find()) {
					fileList.add(name);
				}

			}

			if (null != fileList && !fileList.isEmpty()) {
				for (String fName : fileList) {
					System.out.println(fName);
				}
			}
		}
		System.out.println("# of files" + fileList.size());

		if (null != fileList && !fileList.isEmpty()) {
			index = new SequenceFile.Reader[fileList.size()];
			for (int i = 0; i < fileList.size(); i++) {
				String fName = fileList.get(i);
				// System.out.println("File name retreived.." + fName);
				index[i] = new SequenceFile.Reader(fs, new Path(inputPath + "/" + fName), fs.getConf());
			}
			long countRecordsAll = 0l;
			SequenceFile.Reader reader = null;

			List<PriorityQueue<PairOfFloatInt>> listQueues = new ArrayList<>();

			for (int z = 0; z < sourceNodeValues.length; z++) {
				listQueues.add(new PriorityQueue<>());
			}

			for (int k = 0; k < index.length; k++) {
				try {
					reader = index[k];
					Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), fs.getConf());
					Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), fs.getConf());
					// System.out.println("key class" + reader.getKeyClass());
					// System.out.println("value class" + reader.getValueClass());
					long position = reader.getPosition();
					while (reader.next(key, value)) {
						countRecordsAll += 1;
						int currentNode = ((IntWritable) key).get();
						ArrayListOfFloatsWritable pageRankList = ((PageRankNode) value).getPageRankList();

						for (int srcN = 0; srcN < sourceNodeValues.length; srcN++) {
							float pRankV = (float) StrictMath.exp(pageRankList.get(srcN));
							PairOfFloatInt pairKV = new PairOfFloatInt();
							pairKV.set(pRankV, currentNode);
							if (listQueues.get(srcN).size() < top
									|| listQueues.get(srcN).peek().getLeftElement() < pRankV) {
								if (listQueues.get(srcN).size() == top) {
									listQueues.get(srcN).remove();
								}
								(listQueues.get(srcN)).add(pairKV);
							}

						}

						position = reader.getPosition();
					}
				} finally {
					IOUtils.closeStream(reader);
				}

			}

			// Writing to the console
			// StringBuffer combinedString = new StringBuffer();
			for (int r = 0; r < listQueues.size(); r++) {
				PriorityQueue<PairOfFloatInt> currentPrQ = listQueues.get(r);
				String srcName = sourceNodeValues[r];
				System.out.println("Source: " + srcName);
				// combinedString.append("Source: " + srcName + NEXT_LINE);
				while (currentPrQ.size() != 0) {
					PairOfFloatInt pairKV = currentPrQ.poll();
					String formattedString = String.format("%.5f %d", pairKV.getLeftElement(),
							pairKV.getRightElement());
					System.out.println(formattedString);
					// combinedString.append(formattedString + NEXT_LINE);
				}
				System.out.println("");
				// combinedString.append(NEXT_LINE);

			}

			// Writing to the file system
			// writeOutputToFile(fs, outPath, combinedString.toString());
			// FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);

		}
	}

	// private void writeOutputToFile(FileSystem fs, String outPath, String toWrite)
	// throws IOException {
	// FSDataOutputStream out = fs.create(new Path(outPath), false);
	// out.writeChars(toWrite);
	// out.close();
	// }

}
