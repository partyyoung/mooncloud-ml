package net.mooncloud.ml.naivebayes.train.featurefrequency;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MrCombiner extends Reducer<Text, LongWritable, Text, LongWritable>
{

	private LongWritable res = new LongWritable();

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
	{
		long val = 0;

		for (LongWritable value : values)
		{
			val += value.get();
		}

		res.set(val);
		context.write(key, res);
	}

}
