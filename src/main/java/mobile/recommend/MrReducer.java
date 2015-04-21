package mobile.recommend;

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
	private class UserItemInfo
	{
		String date_ymd;
		long time_ymd, pv, collect, cart, trade, continuation;
	}

	private Record res = null;

	private String date_ymd;
	private long time_ymd;

	private UserItemInfo info1, info3, info7, info15, info30;

	private static final String[] HOURSTRING = new String[]
	{
			"00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"
	};

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		res = new Record(context.getConfiguration().get("mapred.output.schema"));
		date_ymd = context.getConfiguration().get("date_ymd");
		try
		{
			time_ymd = SomeStaticUtils.DATEFORMAT3.parse(date_ymd).getTime() / 1000;
			date_ymd = SomeStaticUtils.DATEFORMAT4.format(time_ymd * 1000);

			info1 = new UserItemInfo();
			info1.time_ymd = time_ymd - 24 * 60 * 60;
			info1.date_ymd = SomeStaticUtils.DATEFORMAT4.format(info1.time_ymd * 1000);

			info3 = new UserItemInfo();
			info3.time_ymd = time_ymd - 3 * 24 * 60 * 60;
			info3.date_ymd = SomeStaticUtils.DATEFORMAT4.format(info3.time_ymd * 1000);

			info7 = new UserItemInfo();
			info7.time_ymd = time_ymd - 7 * 24 * 60 * 60;
			info7.date_ymd = SomeStaticUtils.DATEFORMAT4.format(info7.time_ymd * 1000);

			info15 = new UserItemInfo();
			info15.time_ymd = time_ymd - 15 * 24 * 60 * 60;
			info15.date_ymd = SomeStaticUtils.DATEFORMAT4.format(info15.time_ymd * 1000);

			info30 = new UserItemInfo();
			info30.time_ymd = time_ymd - 30 * 24 * 60 * 60;
			info30.date_ymd = SomeStaticUtils.DATEFORMAT4.format(info30.time_ymd * 1000);
		}
		catch (ParseException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void userItemDailyPV(Tuple key, Iterable<Record> values, Context context) throws IOException, InterruptedException
	{
		String[] user_item = key.get(0).toString().split("\001");
		String user = user_item[0];
		String item = user_item[1];

		long userItemPV = 0; // pv
		String lastVisitTime = "", firstVisitTime = null, tradeTime = null; // 最后一次访问时间
		int collect = 0, cart = 0, buy = 0, target = 0; // 收藏,购物车,购买
		HashMap<String, Long> userItemHourPV = new HashMap<String, Long>();
		HashMap<Long, Long> userItemDailyPV = new HashMap<Long, Long>();

		for (Record value : values)
		{
			try
			{
				String date = value.get("time").toString();
				long time = SomeStaticUtils.DATEFORMAT2.parse(date).getTime() / 1000;
				if (date.startsWith(date_ymd))
				{// target
					if (target == 0 && "4".equals(value.get("behavior_type").toString()))
					{
						target = 1;
					}
				}
				else
				{
					if ("1".equals(value.get("behavior_type").toString()))
					{
						long pv = 1, daytime = SomeStaticUtils.DATEFORMAT4.parse(date.split(" ")[0]).getTime() / 1000;
						if (userItemDailyPV.containsKey(daytime))
						{
							pv += userItemDailyPV.get(daytime);
						}
						userItemDailyPV.put(daytime, pv);
					}
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}

		StringBuilder sb = new StringBuilder();
		sb.append(user).append(",");
		sb.append(item);
		long day = info30.time_ymd;
		for (int i = 0; i < 30; i++)
		{
			long inc = 0, daytime = day + i * 24 * 60 * 60;
			if (userItemDailyPV.containsKey(daytime))
			{
				inc = userItemDailyPV.get(daytime);
			}
			sb.append(",").append(inc);
		}

		// res.set("user_id", new Text(user_item[0]));
		// res.set("item_id", new Text(user_item[1]));
		// res.set("user_id", key);
		// res.set("pv", sum);
		res.set("result", new Text(sb.toString()));
		context.write(res, NullWritable.get());
	}

	private void tianchiNO1(Tuple key, Iterable<Record> values, Context context) throws IOException, InterruptedException
	{
		String[] user_item = key.get(0).toString().split("\001");
		String user = user_item[0];
		String item = user_item[1];

		long userItemPV = 0; // pv
		String lastVisitTime = "", firstVisitTime = null, tradeTime = null; // 最后一次访问时间
		int collect = 0, cart = 0, buy = 0, target = 0; // 收藏,购物车,购买
		HashMap<String, Long> userItemHourPV = new HashMap<String, Long>();

		for (Record value : values)
		{
			try
			{
				String date = value.get("time").toString();
				long time = SomeStaticUtils.DATEFORMAT2.parse(date).getTime();
				if (date.startsWith(date_ymd))
				{// target
					if (target == 0 && "4".equals(value.get("behavior_type").toString()))
					{
						target = 1;
					}
				}
				else
				{
					lastVisitTime = value.get("time").toString();
					if ("1".equals(value.get("behavior_type").toString()))
					{
						userItemPV += 1;
						if (firstVisitTime == null)
						{
							firstVisitTime = lastVisitTime;
						}
					}
					if (collect == 0 && "2".equals(value.get("behavior_type").toString()))
					{
						collect = 1;
					}
					if (cart == 0 && "3".equals(value.get("behavior_type").toString()))
					{
						cart = 1;
					}
					if (buy == 0 && "4".equals(value.get("behavior_type").toString()))
					{
						buy = 1;
						tradeTime = lastVisitTime;
					}
					if (date.startsWith(info1.date_ymd))
					{
						String hour = date.split(" ")[1];
						long inc = 0;
						if (userItemHourPV.containsKey(hour))
						{
							inc = userItemHourPV.get(hour);
						}
						userItemHourPV.put(hour, inc + 1);
					}
				}
			}
			catch (Exception e)
			{
				// e.printStackTrace();
			}
		}

		long diff = 0, lasttime = 0;
		if (tradeTime != null && firstVisitTime != null)
		{// 从关注商品到购买商品的时间跨度
			try
			{
				diff = (SomeStaticUtils.DATEFORMAT2.parse(tradeTime).getTime() - SomeStaticUtils.DATEFORMAT2.parse(firstVisitTime).getTime()) / 1000;
			}
			catch (ParseException e)
			{
				// e.printStackTrace();
			}
		}
		diff = diff < 0 ? 0 : diff;

		try
		{
			lasttime = (SomeStaticUtils.DATEFORMAT4.parse(date_ymd).getTime() - SomeStaticUtils.DATEFORMAT2.parse(lastVisitTime).getTime()) / 1000;
		}
		catch (ParseException e)
		{
			// e.printStackTrace();
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
		for (String hour : HOURSTRING)
		{
			long inc = 0;
			if (userItemHourPV.containsKey(hour))
			{
				inc = userItemHourPV.get(hour);
			}
			sb.append(",").append(inc);
		}
		sb.append(",").append(diff);
//		sb.append(",").append(target);

		// res.set("user_id", new Text(user_item[0]));
		// res.set("item_id", new Text(user_item[1]));
		// res.set("user_id", key);
		// res.set("pv", sum);
		res.set("result", new Text(sb.toString()));
		context.write(res, NullWritable.get());
	}

	@Override
	public void reduce(Tuple key, Iterable<Record> values, Context context) throws IOException, InterruptedException
	{
		userItemDailyPV(key, values, context);
//		tianchiNO1(key, values, context);
	}
}
