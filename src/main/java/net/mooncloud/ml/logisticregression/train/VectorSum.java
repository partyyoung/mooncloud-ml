package net.mooncloud.ml.logisticregression.train;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import net.mooncloud.Record;
import net.mooncloud.mapreduce.TableReducer;
import net.mooncloud.util.Yaml;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class VectorSum extends TableReducer<IntWritable, Text>
{
	static Yaml yaml = new Yaml();
	private static final int D = 10; // Number of dimensions

	private Record res = null;
	double[] gradient = new double[D + 1];

	private double[] weights;

	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		res = new Record(context.getConfiguration().get("mapred.output.schema"));

		String[] weightstr = context.getConfiguration().getStrings("weights");
		weights = new double[D + 1];
		for (int i = 0; i < D + 1; i++)
		{
			weights[i] = Double.parseDouble(weightstr[i]);
		}
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		for (Text record : values)
		{
			ArrayList<Double> array = (ArrayList<Double>) yaml.load(record.toString());
			for (int j = 0; j < D + 1; j++)
			{
				// result[j] += ((DoubleWritable) array[j]).get();
				gradient[j] += array.get(j);
			}
		}

		for (int j = 0; j < D + 1; j++)
		{
			weights[j] -= gradient[j];
		}

		res.set(0, new Text(Arrays.toString(weights)));
		context.write(res, NullWritable.get());
	}
}
