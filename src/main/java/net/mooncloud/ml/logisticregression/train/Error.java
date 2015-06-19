package net.mooncloud.ml.logisticregression.train;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.mapreduce.TableMapper;
import net.mooncloud.ml.logisticregression.train.MrLogisticRegression.DataPoint;
import net.mooncloud.ml.logisticregression.train.MrLogisticRegression.ParsePoint;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

public class Error extends TableMapper<IntWritable, DoubleWritable>
{
	private static final int D = 10; // Number of dimensions
	private double[] weights;

	public Double call(DataPoint p)
	{
		double z = weights[D];
		for (int i = 0; i < D; i++)
		{
			z += weights[i] * p.x[i];
		}

		double y = 1 / (1 + Math.exp(-z));
		return y - p.y;
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		String[] weightstr = context.getConfiguration().getStrings("weights");
		weights = new double[D + 1];
		for (int i = 0; i < D + 1; i++)
		{
			weights[i] = Double.parseDouble(weightstr[i]);
		}
	}

	@Override
	public void map(InputSplitFile recordNum, Record record, Context context) throws IOException, InterruptedException
	{
		String line = record.get(0).toString();
		DataPoint p = ParsePoint.call(line);
		double error = call(p);
		context.write(new IntWritable(0), new DoubleWritable(error));
	}
}