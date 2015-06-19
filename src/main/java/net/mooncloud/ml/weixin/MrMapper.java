package net.mooncloud.ml.weixin;

import java.io.IOException;
import java.text.ParseException;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.io.Tuple;
import net.mooncloud.mapreduce.TableMapper;
import net.mooncloud.util.SomeStaticUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MrMapper extends TableMapper<Tuple, LongWritable>
{

	private Tuple key = new Tuple(2);

	@Override
	public void map(InputSplitFile recordNum, Record record, Context context) throws IOException, InterruptedException
	{
		String r[] = record.get("record").toString().split("\\|");

		key.set(0, new Text(r[0] + "\t" + r[1] + "\t" + r[2]));
		key.set(1, new Text(r[5]));

		long timestamp = 0;
		try
		{
			timestamp = SomeStaticUtils.DATEFORMAT1.parse(r[5].split("\\.")[0]).getTime();
		}
		catch (ParseException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		context.write(key, new LongWritable(timestamp));
	}
}
