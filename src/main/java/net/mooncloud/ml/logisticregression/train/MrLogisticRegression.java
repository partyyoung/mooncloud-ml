package net.mooncloud.ml.logisticregression.train;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.regex.Pattern;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.mapreduce.TableMapper;
import net.mooncloud.mapreduce.TableReducer;
import net.mooncloud.util.Yaml;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MrLogisticRegression
{

	static Yaml yaml = new Yaml();

	private static final int D = 10; // Number of dimensions
	private static final Random rand = new Random(42);

	static void showWarning()
	{
		String warning = "WARN: This is a naive implementation of Logistic Regression " + "and is given as an example!\n"
				+ "Please use either org.apache.spark.mllib.classification.LogisticRegressionWithSGD "
				+ "or org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS " + "for more conventional use.";
		System.err.println(warning);
	}

	static class DataPoint implements Serializable
	{
		DataPoint(double[] x, double y)
		{
			this.x = x;
			this.y = y;
		}

		double[] x;
		double y;
	}

	static class ParsePoint
	{
		private static final Pattern SPACE = Pattern.compile(" ");

		public static DataPoint call(String line)
		{
			String[] tok = SPACE.split(line);
			double y = Double.parseDouble(tok[0]);
			double[] x = new double[D];
			for (int i = 0; i < D; i++)
			{
				x[i] = Double.parseDouble(tok[i + 1]);
			}
			return new DataPoint(x, y);
		}
	}

	public static class ComputeGradient extends TableMapper<IntWritable, ArrayWritable>
	{
		private static final Pattern SPACE = Pattern.compile(" ");
		private double[] weights;

		public DoubleWritable[] call(DataPoint p)
		{
			DoubleWritable[] gradient = new DoubleWritable[D + 1];
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
				gradient[i].set((y - p.y) * p.x[i]);
			}
			gradient[D].set(y - p.y);
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
			DoubleWritable[] gradient = call(p);
			ArrayWritable ArrayWritable = new ArrayWritable(DoubleWritable.class);
			ArrayWritable.set(gradient);
			context.write(new IntWritable(0), ArrayWritable);
		}
	}

	public static class VectorSum extends TableReducer<IntWritable, ArrayWritable>
	{
		public double[] call(double[] a, double[] b)
		{
			double[] result = new double[D + 1];
			for (int j = 0; j < D + 1; j++)
			{
				result[j] = a[j] + b[j];
			}
			return result;
		}

		private Record res = null;
		double[] result = new double[D + 1];

		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			res = new Record(context.getConfiguration().get("mapred.output.schema"));
		}

		@Override
		public void reduce(IntWritable key, Iterable<ArrayWritable> values, Context context) throws IOException, InterruptedException
		{
			for (ArrayWritable record : values)
			{
				Writable[] array = record.get();
				for (int j = 0; j < D + 1; j++)
				{
					result[j] += ((DoubleWritable) array[j]).get();
				}
				res.set(0, new Text(yaml.dump(record)));
				context.write(res, NullWritable.get());
			}
		}
	}

	public static double dot(double[] a, double[] b)
	{
		double x = 0;
		for (int i = 0; i < D; i++)
		{
			x += a[i] * b[i];
		}
		return x;
	}

	public static void printWeights(double[] a)
	{
		System.out.println(Arrays.toString(a));
	}

	public static class Error extends TableMapper<IntWritable, DoubleWritable>
	{
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

	public static class ErrorSum extends TableReducer<IntWritable, DoubleWritable>
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

	public static void main(String[] args)
	{
		double[] array =
		{
				1, 2, 3
		};
		System.out.println((ArrayList<String>) yaml.load(yaml.dump(array)));
	}
}
