package tianchi.ant;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;

import net.mooncloud.Record;
import net.mooncloud.io.Tuple;
import net.mooncloud.mapreduce.TableReducer;
import net.mooncloud.util.SomeStaticUtils;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class MrReducer extends TableReducer<Tuple, Record>
{
	private Record res = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		res = new Record(context.getConfiguration().get("mapred.output.schema"));
	}

	@Override
	public void reduce(Tuple key, Iterable<Record> values, Context context) throws IOException, InterruptedException
	{
		for (Record value : values)
		{
			res.set("result", new Text(value.toString(",")));
			context.write(res, NullWritable.get());
		}
	}
}
