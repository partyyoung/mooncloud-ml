package net.mooncloud.ml.roughset;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ParallelFeatureReduce implements Tool
{

	static class FSMapper extends Mapper<LongWritable, Text, IntIntPair, Text>
	{

		final private String cvs = ",";

		private int m = 0;
		private String featureSelect = " ";
		private String featureLeft = "";
		private String[] featureLeftStringArray;
		private int[] featureSelectIntArray = new int[0];
		private int[] featureLeftIntArray;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line = value.toString().trim().split(cvs);

			if (line.length == m + 1)
			{

				StringBuffer sb = new StringBuffer();

				for (int i = 0; i < featureSelectIntArray.length; i++)
				{
					int k = featureSelectIntArray[i];
					sb.append(line[k - 1] + " ");
				}

				for (int i = 0; i < featureLeftStringArray.length; i++)
				{

					int k = featureLeftIntArray[i];
					sb.append(line[k - 1]);

					context.write(new IntIntPair(Integer.parseInt(featureLeftStringArray[i]), sb.toString().hashCode()), new Text(line[m]));

					sb.delete(sb.length() - line[k - 1].length(), sb.length());
				}
			}
		}

		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();

			m = conf.getInt("featureNumber", -1);
			featureSelect = conf.get("featureSelect");
			featureLeft = conf.get("featureLeft");

			featureLeftStringArray = featureLeft.trim().split(" ");
			featureLeftIntArray = new int[featureLeftStringArray.length];
			for (int i = 0; i < featureLeftStringArray.length; i++)
				featureLeftIntArray[i] = Integer.parseInt(featureLeftStringArray[i]);

			String[] featureSelectStringArray;
			if (!featureSelect.equals(" ") && !featureSelect.equals(""))
			{
				featureSelectStringArray = featureSelect.trim().split(" ");
				featureSelectIntArray = new int[featureSelectStringArray.length];
				for (int i = 0; i < featureSelectStringArray.length; i++)
					featureSelectIntArray[i] = Integer.parseInt(featureSelectStringArray[i]);
			}

		}
	}

	static class FSMapper1 extends Mapper<LongWritable, Text, IntIntPair, TextLongPair>
	{

		final private String cvs = ",";

		private int m = 0;
		private String featureSelect = " ";
		private String featureLeft = "";
		private String[] featureLeftStringArray;
		private int[] featureSelectIntArray = new int[0];
		private int[] featureLeftIntArray;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line = value.toString().trim().split(cvs);

			if (line.length == m + 1)
			{

				for (int i = 0; i < featureSelectIntArray.length; i++)
				{
					int l = featureSelectIntArray[i];
					StringBuffer sb = new StringBuffer();

					for (int j = 0; j < featureSelectIntArray.length; j++)
					{
						if (i == j)
						{
							continue;
						}

						int k = featureSelectIntArray[j];
						sb.append(line[k - 1] + " ");
					}
					context.write(new IntIntPair(l, sb.toString().hashCode()), new TextLongPair(line[m], new Long(1)));
				}
			}
		}

		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();

			m = conf.getInt("featureNumber", -1);
			featureSelect = conf.get("featureSelect");
			featureLeft = conf.get("featureLeft");

			featureLeftStringArray = featureLeft.trim().split(" ");
			featureLeftIntArray = new int[featureLeftStringArray.length];
			for (int i = 0; i < featureLeftStringArray.length; i++)
				featureLeftIntArray[i] = Integer.parseInt(featureLeftStringArray[i]);

			String[] featureSelectStringArray;
			if (!featureSelect.equals(" ") && !featureSelect.equals(""))
			{
				featureSelectStringArray = featureSelect.trim().split(" ");
				featureSelectIntArray = new int[featureSelectStringArray.length];
				for (int i = 0; i < featureSelectStringArray.length; i++)
					featureSelectIntArray[i] = Integer.parseInt(featureSelectStringArray[i]);
			}

		}
	}

	static class FSCombiner extends Reducer<IntIntPair, TextLongPair, IntIntPair, TextLongPair>
	{
		public void reduce(IntIntPair key, Iterable<TextLongPair> values, Context context) throws IOException, InterruptedException
		{
			Iterator<TextLongPair> iteValues = values.iterator();
			HashMap<String, Long> classCount = new HashMap<String, Long>();
			while (iteValues.hasNext())
			{
				TextLongPair value = iteValues.next();
				String c = value.getFirst().toString();
				long count = value.getSecond().get();
				if (classCount.containsKey(c))
				{
					count = classCount.get(c).longValue() + count;
				}
				classCount.put(c, count);
			}

			Iterator iter = classCount.entrySet().iterator();
			while (iter.hasNext())
			{
				Map.Entry entry = (Map.Entry) iter.next();
				String vj = entry.getKey().toString();
				Long vjc = Long.parseLong(entry.getValue().toString());

				context.write(key, new TextLongPair(vj, vjc));
			}
		}
	}

	static class FSConsistencyReducer1 extends Reducer<IntIntPair, TextLongPair, IntWritable, LongWritable>
	{

		private long postive = 0;
		private long current = 0;
		private boolean consistence = true;
		private IntWritable f = null;

		public void reduce(IntIntPair key, Iterable<TextLongPair> values, Context context) // 注意：这里要用Iterable，不能用Iterator，不知道为什么。
				throws IOException, InterruptedException
		{

			Iterator<TextLongPair> iteValues = values.iterator();
			HashMap<String, Long> classCount = new HashMap<String, Long>();
			while (iteValues.hasNext())
			{
				TextLongPair value = iteValues.next();
				String c = value.getFirst().toString();
				long count = value.getSecond().get();
				if (classCount.containsKey(c))
				{
					count = classCount.get(c).longValue() + count;
				}
				classCount.put(c, count);
			}

			// get Consistency Number of Instances
			Iterator iter = classCount.entrySet().iterator();
			long maxCount = 0;
			String groupClass = null;
			while (iter.hasNext())
			{
				Map.Entry entry = (Map.Entry) iter.next();
				String vj = entry.getKey().toString();
				Long vjc = Long.parseLong(entry.getValue().toString());

				if (maxCount < vjc.longValue())
				{
					maxCount = vjc.longValue();
					groupClass = vj;
				}
			}

			f = key.getFirst();
			current = maxCount;

			context.write(f, new LongWritable(current));
		}
	}

	static class FSConsistencyReducer extends Reducer<IntIntPair, Text, IntWritable, LongWritable>
	{

		private long postive = 0;
		private long current = 0;
		private boolean consistence = true;
		private IntWritable f = null;

		public void reduce(IntIntPair key, Iterable<Text> values, Context context) // 注意：这里要用Iterable，不能用Iterator，不知道为什么。
				throws IOException, InterruptedException
		{

			Iterator<Text> iteValues = values.iterator();
			String groupClass = null;
			HashMap<String, Long> classCount = new HashMap<String, Long>();
			while (iteValues.hasNext())
			{
				String c = new Text(iteValues.next()).toString(); // 注意：这里要new出来，否则下面equals的时候总是为true。和Java的引用有关系。
				long count;
				if (classCount.containsKey(c))
				{
					count = classCount.get(c).longValue() + 1;
				}
				else count = 1;
				classCount.put(c, count);
			}

			// get Consistency Number of Instances
			Iterator iter = classCount.entrySet().iterator();
			long maxCount = Long.MIN_VALUE;
			while (iter.hasNext())
			{
				Map.Entry entry = (Map.Entry) iter.next();
				String vj = entry.getKey().toString();
				Long vjc = Long.parseLong(entry.getValue().toString());

				if (maxCount < vjc.longValue())
				{
					maxCount = vjc.longValue();
					groupClass = vj;
				}
			}

			f = key.getFirst();
			current = maxCount;

			context.write(f, new LongWritable(current));
		}
	}

	static class FSDependencyReducer extends Reducer<IntTextPair, Text, IntWritable, LongWritable>
	{

		private long postive = 0;
		private long current = 0;
		private boolean consistence = true;
		private IntWritable f = null;

		public void reduce(IntTextPair key, Iterable<Text> values, Context context) // 注意：这里要用Iterable，不能用Iterator，不知道为什么。
				throws IOException, InterruptedException
		{

			Iterator<Text> iteValues = values.iterator();
			f = key.getFirst();
			Text v = new Text(iteValues.next());
			current = 0;
			consistence = true;
			while (iteValues.hasNext())
			{
				Text record = new Text(iteValues.next()); // 注意：这里要new出来，否则下面equals的时候总是为true。和Java的引用有关系。
				if (v.equals(record.toString()))
					current++;
				else
				{
					consistence = false;
					current = 0;
					break;
				}
			}
			if (consistence)
				// postive += current;

				context.write(f, new LongWritable(current));
		}
	}

	static class SIGMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line = value.toString().split("\t");
			context.write(new IntWritable(Integer.parseInt(line[0])), new LongWritable(Long.parseLong(line[1])));
		}
	}

	static class SIGReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable>
	{
		// private long maxSig = 0;
		// private int fs = -1;

		public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sig = 0;
			for (LongWritable value : values)
			{
				sig += value.get();
			}

			context.write(key, new LongWritable(sig));
		}

		public void cleanup(Context context)
		{
			// Configuration conf = context.getConfiguration();
			// System.out.println(fs + " " + maxSig);
			// conf.setInt("MIF", fs);
			// conf.setLong("SIGF", maxSig);
		}
	}

	static class FeaturePartitioner extends Partitioner<IntIntPair, TextLongPair>
	{
		public int getPartition(IntIntPair key, TextLongPair value, int numPartitions)
		{
			// return Math.abs(key.hashCode()) % numPartitions;
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	static class FeatureGroupComparator implements RawComparator<IntTextPair>
	{

		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5)
		{
			return WritableComparator.compareBytes(arg0, arg1, Integer.SIZE / 8, arg3, arg4, Integer.SIZE / 8);
		}

		@Override
		public int compare(IntTextPair o1, IntTextPair o2)
		{
			int cmp = o1.getFirst().compareTo(o2.getFirst());
			// System.out.println("FeatureGroupComparator:" + cmp);
			return cmp;
		}
	}

	@Override
	public Configuration getConf()
	{
		return null;
	}

	@Override
	public void setConf(Configuration arg0)
	{

	}

	private int getFeatureNumber(String inputpath, Configuration conf) throws IOException
	{
		int M = 0;

		FileSystem hdfs = FileSystem.get(URI.create(inputpath), conf);
		FileStatus[] fstatus = hdfs.listStatus(new Path(inputpath));
		FSDataInputStream in = null;
		try
		{

			for (int i = 0; i < fstatus.length; i++)
			{
				Path p = fstatus[i].getPath();
				if (!hdfs.isFile(p))
					continue;
				in = hdfs.open(p);
				String line;
				if ((line = in.readLine()) != null)
				{
					String[] fc = line.split(",");
					M = fc.length;
					break;
				}
			}

		}
		finally
		{
			try
			{
				in.close();
			}
			catch (Exception e)
			{
			}
		}
		return M;
	}

	@Override
	public int run(String[] args) throws Exception
	{
		if (4 != args.length)
		{
			System.err.println("Usage: ParallelFeatureSelect <taskTrackers> <input path> <output tmp path> <output path>");
			System.exit(-1);
		}

		int taskTrackers = 0;

		taskTrackers = (taskTrackers = Integer.parseInt(args[0])) <= 2 ? 2 : taskTrackers;
		String inputpath = args[1];
		String outputpath = args[2];
		String outputpathFinal = args[3];

		Configuration conf = new Configuration();
		// JobConf conf = new JobConf();

		// 设置 MR 本地运行环境
		conf.set("mapred.job.tracker", "local");
		conf.set("fs.default.name", "file:///home/yangjd/Documents/workspace/ParallelFeatureSelect/");
		// System.setProperty("hadoop.home.dir",
		// "file:///D:/My Documents/Downloads/hadoop-2.3.0");
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

		int M = getFeatureNumber(inputpath, conf);

		int m = M - 1;

		if (M < 2)
		{
			System.err.println("ERROR: Input DataSet Illegal");
			System.exit(-1);
		}

		// setConf
		String featureSelect = " ";
		String featureLeft = "";
		int MIF = -1;
		long SIGF = 0;
		long sig = 14;
		String sigStr = "";
		StringBuffer sb = new StringBuffer();
		for (int i = 1; i <= m; i++)
		{
			sb.append(i + " ");
		}
		featureLeft = sb.toString().trim();
		featureSelect = "1 2 3 4";

		// set global attributes
		conf.setInt("featureNumber", m);
		conf.set("featureSelect", featureSelect);
		conf.set("featureLeft", featureLeft);
		conf.setInt("MIF", MIF);
		conf.setLong("SIGF", SIGF);

		// set Compression
		// conf.setBoolean("mapred.output.compress", true);
		// conf.setClass("mapred.output.compression.codec", GzipCodec.class,
		// CompressionCodec.class);
		// conf.setCompressMapOutput(true);
		// conf.setMapOutputCompressorClass(GzipCodec.class);

		// 开始迭代
		boolean iteration = true;
		while (iteration)
		{

			Job job1 = new Job(conf);
			job1.setJobName("evaluate feature subset");
			job1.setJarByClass(ParallelFeatureReduce.class);

			FileInputFormat.addInputPath(job1, new Path(inputpath));
			FileOutputFormat.setOutputPath(job1, new Path(outputpath));

			job1.setMapperClass(FSMapper1.class);
			// job1.setReducerClass(FSDependencyReducer.class);
			job1.setCombinerClass(FSCombiner.class);
			job1.setReducerClass(FSConsistencyReducer1.class);

			job1.setPartitionerClass(FeaturePartitioner.class);
			// job1.setGroupingComparatorClass(FeatureGroupComparator.class);

			int numReduceTasks = Math.min(m, (int) (0.95 * taskTrackers));

			job1.setNumReduceTasks(numReduceTasks); // (m + 1) / 2

			job1.setOutputKeyClass(IntIntPair.class);
			job1.setOutputValueClass(TextLongPair.class);

			FileSystem fstm = FileSystem.get(URI.create(outputpath), conf);
			Path outDir = new Path(outputpath);
			fstm.delete(outDir, true);

			job1.waitForCompletion(true);

			//
			Job job2 = new Job(conf);
			job2.setJobName("catch most important feature");
			job2.setJarByClass(ParallelFeatureReduce.class);
			FileInputFormat.addInputPath(job2, new Path(outputpath));
			FileOutputFormat.setOutputPath(job2, new Path(outputpathFinal));

			job2.setMapperClass(SIGMapper.class);
			job2.setCombinerClass(SIGReducer.class);
			job2.setReducerClass(SIGReducer.class);

			job2.setNumReduceTasks(1);

			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(LongWritable.class);

			fstm = FileSystem.get(URI.create(outputpathFinal), conf);
			outDir = new Path(outputpathFinal);
			fstm.delete(outDir, true);

			job2.waitForCompletion(true);

			// delete temporary files
			// fstm = FileSystem.get(URI.create(outputpath), conf);
			// outDir = new Path(outputpath);
			// fstm.delete(outDir, true);

			// get Most Important Feature
			String fs = "";
			long fsSig = 0;
			{
				String inputPath = outputpathFinal;
				FileSystem hdfs = FileSystem.get(URI.create(inputPath), conf);
				FileStatus[] fstatus = hdfs.listStatus(new Path(inputPath));
				FSDataInputStream in = null;
				try
				{
					// HashMap<String, Long> featureCount = new HashMap<String,
					// Long>();
					for (int i = 0; i < fstatus.length; i++)
					{
						Path p = fstatus[i].getPath();
						if (!hdfs.isFile(p))
							continue;
						in = hdfs.open(p);
						String line;
						while ((line = in.readLine()) != null)
						{
							String[] fc = line.split("\t");
							String f = fc[0];
							Long c = Long.parseLong(fc[1]);
							// if (featureCount.containsKey(f))
							// {
							// c = new Long(c.longValue()
							// + featureCount.get(f).longValue());
							// }
							// featureCount.put(f, c);
							if (fsSig < c.longValue())
							{
								fs = f;
								fsSig = c.longValue();
							}
						}
					}
					//
					// Iterator iter = featureCount.entrySet().iterator();
					// while (iter.hasNext())
					// {
					// Map.Entry entry = (Map.Entry) iter.next();
					// String fsT = entry.getKey().toString();
					// long total = Long
					// .parseLong(entry.getValue().toString());
					//
					// if (fsSig < total)
					// {
					// fs = fsT;
					// fsSig = total;
					// }
					// }
				}
				finally
				{
					try
					{
						in.close();
					}
					catch (Exception e)
					{
					}
				}
			}

			// fs = conf.getInt("MIF", -1) + "";
			// fsSig = conf.getLong("SIGF", 0);
			//
			// System.out.println(fs + " " + fsSig);

			if (fsSig < sig)
			{
				iteration = false;
				// continue;
			}
			else
			{
				// featureSelect = (featureSelect + " " + fs).trim();
				String[] featureSelectStringArray = featureSelect.trim().split(" ");
				StringBuilder sbb = new StringBuilder();
				for (int i = 0; i < featureSelectStringArray.length; i++)
				{
					if (!fs.equals(featureSelectStringArray[i]))
						sbb.append(featureSelectStringArray[i] + " ");
				}
				featureSelect = sbb.toString().trim();

				// sig = fsSig;
				sigStr += fsSig + " ";

				// featureLeft = (" " + featureLeft + " ").replaceFirst(" " + fs
				// + " ", " ").trim();

				conf.set("featureSelect", featureSelect);
				// conf.set("featureLeft", featureLeft);
			}
			if (--m == 0)
			{
				iteration = false;
				// continue;
			}
			System.out.println(featureSelect);
			System.out.println(sigStr);
			// System.out.println(featureLeft);
			System.out.println();
		}
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		long start = System.nanoTime();
		int exitCode = ToolRunner.run(new ParallelFeatureReduce(), args);
		long end = System.nanoTime();
		System.out.println("time = " + (end - start) / 1e9 + " seconds");
		System.exit(exitCode);
	}

}
