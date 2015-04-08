package net.mooncloud.ml.naivebayes.train.targetprobability;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.mapreduce.TableReducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class MrReducer extends TableReducer<Text, LongWritable> {

	private LongWritable sum = new LongWritable();
	private DoubleWritable pro = new DoubleWritable();
	private Record res = null;

	private long size = 1;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		res = new Record(context.getConfiguration().get("mapred.output.schema"));
		if (context.nextKey()) {
			for (LongWritable value : context.getValues()) {
				size = value.get();
			}
		}
	}

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		long val = 0;
		for (LongWritable value : values) {
			val += value.get();
		}
		sum.set(val);
		pro.set(((double) val) / size);

		res.set("target", key);
		res.set("pv", sum);
		res.set("probability", pro);
		context.write(res, NullWritable.get());
	}

}
