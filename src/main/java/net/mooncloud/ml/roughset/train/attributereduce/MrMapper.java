package net.mooncloud.ml.roughset.train.attributereduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.io.Tuple;
import net.mooncloud.mapreduce.TableMapper;
import net.mooncloud.util.MD5Hash;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MrMapper extends TableMapper<Tuple, Tuple> {

	private Configuration conf;
	private int[] select, left, evaluate, m;

	private Tuple key = new Tuple(3);
	private Tuple value = new Tuple(2);

	private HashMap<Integer, String> featureLeftValue = new HashMap<Integer, String>();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		select = conf.getInts("feature_select");
		left = conf.getInts("feature_left");
		evaluate = conf.getInts("feature_evaluate");
		m = conf.getInts("target_number");
	}

	@Override
	public void map(InputSplitFile recordNum, Record record, Context context)
			throws IOException, InterruptedException {
		String target = "";
		int n = record.getFields().length;
		if (m.length == 0) {// 第一次默认启动,初始化m
			m = new int[1];
			m[0] = n - 1;
		}
		for (int i : m) {
			target += record.get(i) + ",";
		}
		if (left.length == 0) {// 第一次默认启动,初始化left
			HashSet<Integer> mSet = new HashSet<Integer>(m.length
					+ select.length);
			for (int i : m)
				mSet.add(i);
			if (evaluate.length == 0) {
				evaluate = new int[n - m.length];
				for (int i = 0, j = 0; i < n; i++) {
					if (!mSet.contains(i))
						evaluate[j++] = i;
				}
			}
			for (int i : select)
				mSet.add(i);
			left = new int[evaluate.length - select.length];
			int j = 0;
			for (int i : evaluate) {
				if (!mSet.contains(i))
					left[j++] = i;
			}
		}

		// StringBuffer sb = new StringBuffer();
		// for (int i = 0; i < select.length; i++) {
		// int k = select[i];
		// sb.append(record.get(k) + ",");
		// }

		// String featuresValueString = sb.toString();

		for (int i = 0; i < left.length; i++) {
			int k = left[i];
			featureLeftValue.put(k, record.get(k).toString());
		}
		for (int i = 0; i < left.length; i++) {
			int k = left[i];
			// sb.append(record.get(k));
			featureLeftValue.remove(k);
			String featuresValueString = featureLeftValue.toString();

			key.set(0, new IntWritable(k));
			key.set(1,
					new Text(MD5Hash
							.digest(featuresValueString)
							.toString()));
			key.set(2, new Text(target));
			value.set(0, new Text(target));
			value.set(1, new LongWritable(1L));

			context.write(key, value);

			featureLeftValue.put(k, record.get(k).toString());
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
	}
}
