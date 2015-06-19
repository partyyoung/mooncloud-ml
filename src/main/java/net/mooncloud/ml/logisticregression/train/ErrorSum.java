package net.mooncloud.ml.logisticregression.train;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.mapreduce.TableReducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class ErrorSum extends TableReducer<IntWritable, DoubleWritable>
{
	double E = 0.0;

	private Record res = null;

	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		res = new Record(context.getConfiguration().get("mapred.output.schema"));
	}

	@Override
	public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
	{
		for (DoubleWritable record : values)
		{
			E += Math.pow(record.get(), 2) / 2.0;
		}

		res.set(0, new DoubleWritable(E));
		context.write(res, NullWritable.get());
	}
}