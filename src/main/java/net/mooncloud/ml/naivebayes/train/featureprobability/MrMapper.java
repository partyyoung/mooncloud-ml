package net.mooncloud.ml.naivebayes.train.featureprobability;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.io.Tuple;
import net.mooncloud.mapreduce.TableMapper;

import org.apache.hadoop.io.IntWritable;

public class MrMapper extends TableMapper<Tuple, Tuple> {

	private String tablename;

	private Tuple key = new Tuple(2);
	private Tuple value = new Tuple(1);

	@Override
	public void map(InputSplitFile recordNum, Record record, Context context)
			throws IOException, InterruptedException {
		tablename = recordNum.getTableName();

		if ("weather_targetprobability".equals(tablename)) {
			key.set(0, record.get("target"));
			key.set(1, new IntWritable(0));

			value.set(0, record.get("pv"));
		} else if ("weather_featurefrequency".equals(tablename)) {
			key.set(0, record.get("target"));
			key.set(1, new IntWritable(1));

			value.set(0, record);
		}
		context.write(key, value);
	}
}
