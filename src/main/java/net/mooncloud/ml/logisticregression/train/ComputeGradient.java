package net.mooncloud.ml.logisticregression.train;

import java.io.IOException;
import java.util.regex.Pattern;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.mapreduce.TableMapper;
import net.mooncloud.ml.logisticregression.train.MrLogisticRegression.DataPoint;
import net.mooncloud.ml.logisticregression.train.MrLogisticRegression.ParsePoint;
import net.mooncloud.util.Yaml;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class ComputeGradient extends TableMapper<IntWritable, Text>
{
	static Yaml yaml = new Yaml();
	private static final int D = 10; // Number of dimensions
	private static final Pattern SPACE = Pattern.compile(" ");
	private double[] weights;

	public double[] call(DataPoint p)
	{
		double[] gradient = new double[D + 1];
		double z = weights[D];
		for (int ii = 0; ii < D; ii++)
		{
			z += weights[ii] * p.x[ii];
		}
		double y = 1 / (1 + Math.exp(-z));
		for (int i = 0; i < D; i++)
		{
			// double dot = dot(weights, p.x);
			// gradient[i] = (1 / (1 + Math.exp(-p.y * dot)) - 1) * p.y *
			// p.x[i];
			gradient[i] = (y - p.y) * p.x[i];// new DoubleWritable((y - p.y) *
												// p.x[i]);
		}
		gradient[D] = y - p.y;// new DoubleWritable(y - p.y);
		return gradient;
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
		double[] gradient = call(p);
		// ArrayWritable ArrayWritable = new
		// ArrayWritable(DoubleWritable.class);
		// ArrayWritable.set(gradient);

		context.write(new IntWritable(0), new Text(yaml.dump(gradient)));
	}
}
