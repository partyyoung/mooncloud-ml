package net.mooncloud.ml.roughset.train.attributereduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import net.mooncloud.Record;
import net.mooncloud.io.Tuple;
import net.mooncloud.mapreduce.TableReducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class MrReducer extends TableReducer<Tuple, Tuple> {

	private Record res = null;
	private IntWritable prekey;
	private Text preFeaturesValueString;
	private long sum = 0;

	private HashMap<String, Long> targetPV = new HashMap<String, Long>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		res = new Record(context.getConfiguration().get("mapred.output.schema"));
	}

	@Override
	public void reduce(Tuple key, Iterable<Tuple> values, Context context)
			throws IOException, InterruptedException {
		IntWritable key0 = (IntWritable) key.get(0);
		Text featuresValueString = (Text) key.get(1);

		try {
			if (prekey == null || !prekey.equals(key0)) { // 下一个group
				if (prekey != null) {
					res.set(0, prekey);
					res.set(1, new LongWritable(sum));
					context.write(res, NullWritable.get());
					sum = 0;
				}
				prekey = key0;
			}
			if (preFeaturesValueString == null
					|| !preFeaturesValueString.equals(featuresValueString)) { // 下一个group
				preFeaturesValueString = featuresValueString;
				targetPV.clear();
			}

			for (Tuple value : values) {
				String target = value.get(0).toString();
				long pv = ((LongWritable) value.get(1)).get();
				if (targetPV.containsKey(target)) {
					pv = targetPV.get(target).longValue() + pv;
				}
				targetPV.put(target, pv);
			}

			Iterator<Entry<String, Long>> iter = targetPV.entrySet().iterator();
			long maxCount = 0;
			String groupClass = null;
			while (iter.hasNext()) {
				Entry<String, Long> entry = (Entry<String, Long>) iter.next();
				String vj = entry.getKey().toString();
				Long vjc = Long.parseLong(entry.getValue().toString());

				if (maxCount < vjc.longValue()) {
					maxCount = vjc.longValue();
					groupClass = vj;
				}
			}
			sum += maxCount;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		res.set(0, prekey);
		res.set(1, new LongWritable(sum));
		context.write(res, NullWritable.get());
		sum = 0;
	}
}
