package mobile.recommend;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;

import net.mooncloud.Record;
import net.mooncloud.io.Tuple;
import net.mooncloud.mapreduce.TableReducer;
import net.mooncloud.util.SomeStaticUtils;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class MrReducer extends TableReducer<Tuple, Record> {

	private Record res = null;

	private String date_ymd, date_ymd_1;
	private static final String[] HOURSTRING = new String[] { "00", "01", "02",
			"03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13",
			"14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		res = new Record(context.getConfiguration().get("mapred.output.schema"));
		date_ymd = context.getConfiguration().get("date_ymd");
		try {
			long ymd = SomeStaticUtils.DATEFORMAT3.parse(date_ymd).getTime();
			date_ymd = SomeStaticUtils.DATEFORMAT4.format(ymd);
			ymd -= 24 * 60 * 60 * 1000;
			date_ymd_1 = SomeStaticUtils.DATEFORMAT4.format(ymd);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void reduce(Tuple key, Iterable<Record> values, Context context)
			throws IOException, InterruptedException {
		String[] user_item = key.get(0).toString().split("\001");
		String user = user_item[0];
		String item = user_item[1];

		long userItemPV = 0; // pv
		String lastVisitTime = "", firstVisitTime = null, tradeTime = null; // 最后一次访问时间
		int collect = 0, cart = 0, buy = 0, target = 0; // 收藏,购物车,购买
		HashMap<String, Long> userItemHourPV = new HashMap<String, Long>();

		for (Record value : values) {
			String time = value.get("time").toString();
			if (time.startsWith(date_ymd)) {// target
				if (target == 0
						&& "4".equals(value.get("behavior_type").toString())) {
					target = 1;
				}
			} else {
				lastVisitTime = value.get("time").toString();
				if ("1".equals(value.get("behavior_type").toString())) {
					userItemPV += 1;
					if (firstVisitTime == null) {
						firstVisitTime = lastVisitTime;
					}
				}
				if (collect == 0
						&& "2".equals(value.get("behavior_type").toString())) {
					collect = 1;
				}
				if (cart == 0
						&& "3".equals(value.get("behavior_type").toString())) {
					cart = 1;
				}
				if (buy == 0
						&& "4".equals(value.get("behavior_type").toString())) {
					buy = 1;
					tradeTime = lastVisitTime;
				}
				if (time.startsWith(date_ymd_1)) {
					String hour = time.split(" ")[1];
					long inc = 0;
					if (userItemHourPV.containsKey(hour)) {
						inc = userItemHourPV.get(hour);
					}
					userItemHourPV.put(hour, inc + 1);
				}
			}
		}

		long diff = 0, lasttime = 0;
		if (tradeTime != null && firstVisitTime != null) {// 从关注商品到购买商品的时间跨度
			try {
				diff = SomeStaticUtils.DATEFORMAT2.parse(tradeTime).getTime()
						- SomeStaticUtils.DATEFORMAT2.parse(firstVisitTime)
								.getTime() / 1000;
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		diff = diff < 0 ? 0 : diff;

		try {
			lasttime = (SomeStaticUtils.DATEFORMAT3.parse(date_ymd).getTime() - SomeStaticUtils.DATEFORMAT2
					.parse(lastVisitTime).getTime()) / 1000;
		} catch (ParseException e) {
			e.printStackTrace();
		}

		StringBuilder sb = new StringBuilder();
		sb.append(user).append(",");
		sb.append(item).append(",");
		sb.append(userItemPV).append(",");
		sb.append(lastVisitTime).append(",");
		sb.append(lasttime).append(",");
		sb.append(collect).append(",");
		sb.append(cart).append(",");
		sb.append(buy);
		for (String hour : HOURSTRING) {
			long inc = 0;
			if (userItemHourPV.containsKey(hour)) {
				inc = userItemHourPV.get(hour);
			}
			sb.append(",").append(inc);
		}
		sb.append(",").append(diff);
		sb.append(",").append(target);

		// res.set("user_id", new Text(user_item[0]));
		// res.set("item_id", new Text(user_item[1]));
		// res.set("user_id", key);
		// res.set("pv", sum);
		res.set("result", new Text(sb.toString()));
		context.write(res, NullWritable.get());
	}
}
