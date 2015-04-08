package net.mooncloud.ml.naivebayes.classify;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.mapreduce.TableMapper;

import org.apache.hadoop.io.IntWritable;

public class MrMapper extends TableMapper<IntWritable, Record> {

	private String tablename;

	IntWritable key = new IntWritable();

	@Override
	public void map(InputSplitFile recordNum, Record record, Context context)
			throws IOException, InterruptedException {
		tablename = recordNum.getTableName();

		if ("weather".equals(tablename)) {
			key.set(2);

		} else if ("weather_targetprobability".equals(tablename)) {
			key.set(0);

		} else {
			key.set(1);
		}
		context.write(key, record);
	}
}
