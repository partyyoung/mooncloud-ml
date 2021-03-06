package tianchi.ant;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.io.Tuple;
import net.mooncloud.mapreduce.TableMapper;

import org.apache.hadoop.io.Text;

public class MrMapper extends TableMapper<Tuple, Record> {

	Tuple key = new Tuple(2);

	@Override
	public void map(InputSplitFile recordNum, Record record, Context context)
			throws IOException, InterruptedException {
		if(!record.get("user_id").toString().equals("1"))
			return;
		key.set(0,
				new Text(record.get("user_id") + "\001" + record.get("report_date")));
		key.set(1, record.get("report_date"));
		context.write(key, record);
	}
}
