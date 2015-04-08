package net.mooncloud.ml.naivebayes.train.targetprobability;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.mapreduce.TableMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MrMapper extends TableMapper<Text, LongWritable> {

	@Override
	public void map(InputSplitFile recordNum, Record record, Context context)
			throws IOException, InterruptedException {
		context.write((Text) record.get("play"), new LongWritable(1L));
		context.getCounter("target", "play").increment(1L);
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		context.write(new Text(), new LongWritable(context.getCounter("target", "play").getValue()));
	}
}
