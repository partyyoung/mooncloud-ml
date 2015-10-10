package net.mooncloud.ml.kmeans.train;

import java.io.IOException;
import java.util.Arrays;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.io.Tuple;
import net.mooncloud.mapreduce.TableMapper;
import net.mooncloud.ml.KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MrMapper extends TableMapper<Tuple, Tuple>
{

	private Configuration conf;
	private int[] evaluate;
	private int K;
	private double[][] kmeans2;
	private double[][] kmeans;
	private int[] classInputs;

	private KMeans kMeans;

	private Tuple key = new Tuple(1);
	private Tuple value = new Tuple(2);

	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		conf = context.getConfiguration();
		evaluate = conf.getInts("feature_evaluate");
		K = conf.getInt("K", 2);

		kmeans2 = new double[K][evaluate.length]; // 更新K个中心点
		classInputs = new int[K]; // 样本分类[0, K-1]包含的样本个数

		kMeans = new KMeans();
	}

	@Override
	public void map(InputSplitFile recordNum, Record record, Context context) throws IOException, InterruptedException
	{
		int n = record.getFields().length;
		if (evaluate.length == 0)
		{// 第一次默认启动,初始化evaluate
			evaluate = new int[n];
			for (int i = 0; i < n; i++)
				evaluate[i] = i;

			kmeans2 = new double[K][evaluate.length]; // 更新K个中心点
			classInputs = new int[K]; // 样本分类[0, K-1]包含的样本个数
		}

		double[] input = new double[evaluate.length];// 第i个样本
		for (int i = 0; i < evaluate.length; i++)
		{
			input[i] = Double.parseDouble(record.get(evaluate[i]).toString());
		}

		// 2.1 计算第i个样本与每个中心点的距离,并归类 --map
		int c = kMeans.Identify(input, kmeans);
		// 2.2 更新类别c的中心点 --combine--reduce
		classInputs[c]++;
		for (int k = 0; k < evaluate.length; k++)
		{
			kmeans2[c][k] = (kmeans2[c][k] * (classInputs[c] - 1) + input[k]) / classInputs[c];
		}

		for (int k = 0; k < K; k++)
		{
			key.set(0, new IntWritable(k));
			value.set(0, new Text(Arrays.toString(kmeans2[k])));
			value.set(1, new LongWritable(classInputs[k]));
			context.write(key, value);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
	}
}
