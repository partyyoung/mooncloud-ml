package net.mooncloud.ml.kmeans.train;

import java.io.IOException;

import net.mooncloud.io.Tuple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MrCombiner extends Reducer<Tuple, Tuple, Tuple, Tuple> {

	private Tuple value = new Tuple(2);

	@Override
	protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
			throws IOException, InterruptedException {
		long val = 0;

		for (Tuple value : values) {
			val += ((LongWritable) value.get(1)).get();
		}

		value.set(0, key.get(2));
		value.set(1, new LongWritable(val));

		context.write(key, value);
	}

}
