package net.mooncloud.ml.naivebayes.train.featurefrequency;

import java.io.IOException;

import net.mooncloud.FieldSchema;
import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.mapreduce.TableMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MrMapper extends TableMapper<Text, LongWritable> {

	@Override
	public void map(InputSplitFile recordNum, Record record, Context context)
			throws IOException, InterruptedException {
		String play = record.get("play").toString();
		FieldSchema[] fileds = record.getFields();
		for (int i = 0; i < fileds.length - 1; i++) {
			String feature = fileds[i].name;
			String feature_value = record.get(i).toString();
			context.write(new Text(play + "\001" + feature + "\001"
					+ feature_value), new LongWritable(1L));
		}
//		String outlook = record.get("outlook").toString();
//		String temperature = record.get("temperature").toString();
//		String humidity = record.get("humidity").toString();
//		String windy = record.get("windy").toString();
//		context.write(new Text(play + "\001outlook\001" + outlook),
//				new LongWritable(1L));
//		context.write(new Text(play + "\001temperature\001" + temperature),
//				new LongWritable(1L));
//		context.write(new Text(play + "\001humidity\001" + humidity),
//				new LongWritable(1L));
//		context.write(new Text(play + "\001windy\001" + windy),
//				new LongWritable(1L));
	}
}
