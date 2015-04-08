package net.mooncloud.ml.naivebayes.train.featureprobability;

import java.io.IOException;
import java.util.Iterator;

import net.mooncloud.Record;
import net.mooncloud.io.Tuple;
import net.mooncloud.mapreduce.TableReducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class MrReducer extends TableReducer<Tuple, Tuple> {

	private Record res = null;

	private long target_pv;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		res = new Record(context.getConfiguration().get("mapred.output.schema"));
	}

	@Override
	public void reduce(Tuple key, Iterable<Tuple> values, Context context)
			throws IOException, InterruptedException {
		Text target = (Text) key.get(0);

		Iterator<Tuple> iter = values.iterator();
		if (iter.hasNext()) {
			target_pv = ((LongWritable) iter.next().get(0)).get();
		}

		while (iter.hasNext()) {
			Record record = (Record) iter.next().get(0);

			long pv = ((LongWritable) record.get("pv")).get();

			res.set("target", target);
			res.set("feature", record.get("feature"));
			res.set("feature_value", record.get("feature_value"));
			res.set("pv", record.get("pv"));
			res.set("probability", new DoubleWritable((double) pv / target_pv));
			context.write(res, NullWritable.get());
		}
	}

}
