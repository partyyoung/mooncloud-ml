package net.mooncloud.ml.weixin;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.io.Tuple;
import net.mooncloud.mapreduce.TableReducer;
import net.mooncloud.util.SomeStaticUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class MrReducer extends TableReducer<Tuple, LongWritable>
{

	private Record res = null;

	long pre = 0;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		res = new Record(context.getConfiguration().get("mapred.output.schema"));
	}

	@Override
	public void reduce(Tuple key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
	{
		int i = 1;
		for (LongWritable record : values)
		{
			long cur = record.get();
			if (pre != 0)
			{
				if ((cur - pre) <= 14000)
				{
					i++;
				}
				else
				{
					res.set("result", new Text(key.get(0) + "\t" + SomeStaticUtils.DATEFORMAT1.format(pre) + "\t" + i));
					context.write(res, NullWritable.get());
					i = 1;
				}
			}
			pre = cur;
		}

		res.set("result", new Text(key.get(0) + "\t" + SomeStaticUtils.DATEFORMAT1.format(pre) + "\t" + i));
		context.write(res, NullWritable.get());
		pre = 0;
	}
}
