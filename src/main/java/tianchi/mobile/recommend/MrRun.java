package tianchi.mobile.recommend;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.mooncloud.Record;
import net.mooncloud.mapreduce.lib.input.TableFlatInputFormat;
import net.mooncloud.mapreduce.lib.jobcontrol.ControlledJob;
import net.mooncloud.mapreduce.lib.output.TableOutputFormat;
import net.mooncloud.util.Yaml;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * XXX: 除了 TEST_DATE_YMD 取值以外请勿修改其他代码！M/R 任务相关配置请修改 main/resources/mbp_mr.yaml
 * 配置文件
 * 
 * 本地测试用 M/R 驱动程序，从 M/R 配置文件读入配置完成 M/R 任务构造和运行
 */
@SuppressWarnings("rawtypes")
public class MrRun implements Tool {
	// XXX 本地测试时展开 ${date_ymd} 宏的日期值，格式为 yyyyMMdd
	public static final String TEST_DATE_YMD = "20141218";
	public static final String MBP_MR = "tianchi/recommend/mbp_mr.yaml"; 

	private static final Log LOG = LogFactory.getLog(MrRun.class);

	/**
	 * 表父路径，默认值hdfs根目录hdfs://master:9000
	 */
	public static String TABLE_PARENT = "";

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	private static SimpleDateFormat dayFormat1 = new SimpleDateFormat(
			"yyyy-MM-dd");
	private static SimpleDateFormat dayFormat2 = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	/**
	 * 以给定日期为基准展开 ${date_ymd}、${date_ymd-n} 和 ${date_ymd+n} 宏
	 * 
	 * @param str
	 *            含有待展开宏的字符串
	 * @param dateYmd
	 *            基准日期，对应 ${date_ymd}
	 * @return 展开后的字符串
	 */
	public static String expandMacroDateYmd(String str, Date dateYmd) {
		Pattern dateYmdPat = Pattern
				.compile("\\$\\{\\s*date_ymd\\s*(([+-])\\s*(\\d+))?\\s*\\}");
		String res = str;
		Matcher matcher = dateYmdPat.matcher(res);
		while (matcher.find()) {
			String expr = matcher.group(1);
			String op = matcher.group(2);
			String ndays = matcher.group(3);

			if (org.apache.commons.lang.StringUtils.isEmpty(expr)) {
				// 无日期计算部分，直接展开
				res = matcher.replaceFirst(sdf.format(dateYmd));
				matcher = dateYmdPat.matcher(res);
			} else if ("+".equals(op)) {
				// 基准日期+n 天
				int n = Integer.parseInt(ndays);
				res = matcher.replaceFirst(sdf.format(DateUtils.addDays(
						dateYmd, n)));
				matcher = dateYmdPat.matcher(res);
			} else if ("-".equals(op)) {
				// 基准日期-n 天
				int n = -Integer.parseInt(ndays);
				res = matcher.replaceFirst(sdf.format(DateUtils.addDays(
						dateYmd, n)));
				matcher = dateYmdPat.matcher(res);
			}
		}
		return res;
	}

	public static Map loadMrConf(String confName) throws Exception {
		URL url = MrRun.class.getClassLoader().getResource(confName);
		// URL url = ClassLoader.getSystemResource(confName);
		if (null == url)
			LOG.error("配置文件" + confName + "不存在");
		else
			LOG.info(url);
		InputStream is = MrRun.class.getClassLoader().getResourceAsStream(
				confName);
		// InputStream is = ClassLoader.getSystemResourceAsStream(confName);
		InputStreamReader isr = new InputStreamReader(is,
				Charset.forName("UTF-8"));
		Yaml yaml = new Yaml();
		Map mrConf = (Map) yaml.load(isr);
		return mrConf;
	}

	@SuppressWarnings("unchecked")
	public Job makeMrJob(Map mr_conf, Date dateYmd) throws Exception {
		Job job = new Job(getConf());
		job.setJobName(mr_conf.get("prog_name") + "_" + mr_conf.get("app_key")
				+ "_" + sdf.format(dateYmd) + "_"
				+ dayFormat2.format(System.currentTimeMillis()));
		job.setJarByClass(getClass());

		// mr_classes
		Map classes = (Map) mr_conf.get("mr_classes");

		if (!classes.containsKey("mapper")
				|| !classes.containsKey("mapper_output_key")
				|| !classes.containsKey("mapper_output_value")) {
			System.err.println("配置文件必须指定 mapper 类及其输出 key/value 的类型");
			return null;
		}

		Class<?> mapperClazz = Class.forName((String) classes.get("mapper"));
		Class<?> mapperOutputKeyClazz = Class.forName((String) classes
				.get("mapper_output_key"));
		Class<?> mapperOutputValClazz = Class.forName((String) classes
				.get("mapper_output_value"));

		if (mr_conf.containsKey("min_split_size")) {
			int min_split_size = (Integer) mr_conf.get("min_split_size");
			job.getConfiguration().setLong(
					CombineFileInputFormat.SPLIT_MINSIZE_PERNODE,
					min_split_size * 1024 * 1024);
			job.getConfiguration().setLong(
					CombineFileInputFormat.SPLIT_MINSIZE_PERRACK,
					min_split_size * 1024 * 1024);
		}
		if (mr_conf.containsKey("min_split_size")) {
			int max_split_size = (Integer) mr_conf.get("max_split_size");
			job.getConfiguration().setLong(
					"mapreduce.input.fileinputformat.split.maxsize",
					max_split_size * 1024 * 1024);
		}

		job.setInputFormatClass(TableFlatInputFormat.class);
		job.setMapperClass((Class<? extends Mapper>) mapperClazz);
		job.setMapOutputKeyClass(mapperOutputKeyClazz);
		job.setMapOutputValueClass(mapperOutputValClazz);

		// 增加可选的 reducer
		if (classes.containsKey("reducer")) {
			Class<?> reducerClazz = Class.forName((String) classes
					.get("reducer"));
			job.setReducerClass((Class<? extends Reducer>) reducerClazz);
			job.setNumReduceTasks((Integer) mr_conf.get("reduce_tasks"));
		} else {
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(NullWritable.class);
		}
		// 增加可选的 combiner
		if (classes.containsKey("combiner")) {
			Class<?> combinerClazz = Class.forName((String) classes
					.get("combiner"));
			job.setCombinerClass((Class<? extends Reducer>) combinerClazz);
		}
		// 增加可选的 partitioner
		if (classes.containsKey("partitioner")) {
			Class<?> partitionerClazz = Class.forName((String) classes
					.get("partitioner"));
			job.setPartitionerClass((Class<? extends Partitioner>) partitionerClazz);
		}
		// 增加可选的 output key comparator
		if (classes.containsKey("key_comparator")) {
			Class<?> keyComparatorClazz = Class.forName((String) classes
					.get("key_comparator"));
			job.setSortComparatorClass((Class<? extends RawComparator>) keyComparatorClazz);
		}
		// 增加可选的 output value grouping comparator
		if (classes.containsKey("value_grouping_comparator")) {
			Class<?> valGroupComparatorClazz = Class.forName((String) classes
					.get("value_grouping_comparator"));
			job.setGroupingComparatorClass((Class<? extends RawComparator>) valGroupComparatorClazz);
		}

		// 增加可选的 hdfs 表用户目录
		if (mr_conf.containsKey("table_parent")) {
			TABLE_PARENT = (String) mr_conf.get("table_parent");
			job.getConfiguration().set("table_parent", TABLE_PARENT);
		}

		// table_in
		Map inTabs = (Map) mr_conf.get("table_in");
		if (inTabs.size() == 0) {
			LOG.error("至少要有一个输入表！");
			return null;
		}
		for (Object o : inTabs.entrySet()) {
			Entry entry = (Entry) o;
			String inTabName = ((String) entry.getKey()).replace('.', '/');

			String tabNameStr = StringUtils.escapeString(inTabName);
			String tabNames = job.getConfiguration().get("mapred.input.table");
			job.getConfiguration()
					.set("mapred.input.table",
							tabNames == null ? tabNameStr : tabNames + ","
									+ tabNameStr);

			if (entry.getValue() == null) {
				// 无指定输入表的分区
				TableFlatInputFormat.addInputPath(job, new Path(
						(TABLE_PARENT + inTabName)));
			} else if (entry.getValue() instanceof String) {
				// 指定了输入表的单个分区
				String partStr = (String) entry.getValue();
				// 展开分区信息中日期相关宏
				String expandedPartStr = expandMacroDateYmd(partStr, dateYmd);
				TableFlatInputFormat.addInputPath(job, new Path((TABLE_PARENT
						+ inTabName + "/" + expandedPartStr)));
			} else {
				// 指定了输入表的多个分区列表
				List<String> partLst = (List<String>) entry.getValue();
				for (String partStr : partLst) {
					// 展开分区信息中日期相关宏
					String expandedPartStr = expandMacroDateYmd(partStr,
							dateYmd);
					TableFlatInputFormat
							.addInputPath(job, new Path((TABLE_PARENT
									+ inTabName + "/" + expandedPartStr)));
				}
			}
		}

		// table_out
		if (mr_conf.containsKey("db_out")) {
			Object dbTab = mr_conf.get("db_out");
			String db_table = "";
			if (dbTab instanceof String) {
				db_table = (String) dbTab;
			} else {
				Map outTab = (Map) mr_conf.get("table_out");
				if (outTab.size() != 1) {
					LOG.error("输出表只允许有一个！");
					return null;
				}
				for (Object o : outTab.entrySet()) {
					Entry entry = (Entry) o;
					db_table = (String) entry.getKey();
					if (!(entry.getValue() instanceof String)) {
						LOG.error("只允许指定一个输出表的分区！");
						return null;
					}
					String partStr = (String) entry.getValue();
					String expandedPartStr = expandMacroDateYmd(partStr,
							dateYmd);
					db_table += " WHERE " + expandedPartStr;
				}
			}
			job.getConfiguration().set("db_table", db_table);

			String driver_class = "com.mysql.jdbc.Driver";
			if (mr_conf.containsKey("driver_class")) {
				driver_class = (String) mr_conf.get("driver_class");
			}
			String db_url = (String) mr_conf.get("db_url");
			String db_name = (String) mr_conf.get("db_name");
			String user_name = "root";
			if (mr_conf.containsKey("user_name")) {
				user_name = (String) mr_conf.get("user_name");
			}
			String passwd = "koolma2010";
			if (mr_conf.containsKey("passwd")) {
				passwd = (String) mr_conf.get("passwd");
			}
			String table_name = (String) mr_conf.get("table_name");

			DBConfiguration.configureDB(job.getConfiguration(), driver_class,
					db_url, user_name, passwd);
			String table_schema = (String) mr_conf.get("table_schema");
			job.getConfiguration().set("mapred.output.schema", table_schema);
			String[] s = table_schema.split(",", -1);
			String[] field = new String[s.length];
			for (int i = 0; i < s.length; i++) {
				field[i] = s[i].split(":")[0];
			}
			DBOutputFormat.setOutput(job, table_name, field);

			if (!mr_conf.containsKey("table_out")) {
				job.setOutputKeyClass(Record.class);
				job.setOutputValueClass(NullWritable.class);

				Connection connection = null;
				PreparedStatement statement = null;
				try {
					DBConfiguration dbConf = new DBConfiguration(
							job.getConfiguration());
					connection = dbConf.getConnection();
					connection.setAutoCommit(false);
					statement = connection.prepareStatement("DELETE FROM "
							+ db_table);
					// String delete = "DELETE FROM " + db_table;
					int dc = statement.executeUpdate();
					LOG.info("DELETE FROM " + db_table + " : " + dc);
				} catch (Exception e) {
					try {
						LOG.warn("Connection Rollback");
						connection.rollback();
					} catch (SQLException ex) {
						LOG.warn(org.apache.hadoop.util.StringUtils
								.stringifyException(ex));
					}
					throw new IOException(e.getMessage());
				} finally {
					try {
						statement.close();
						connection.close();
					} catch (SQLException ex) {
						throw new IOException(ex.getMessage());
					}
				}
			} else {
				job.getConfiguration().setBoolean("table_db_out", true);
			}
		}

		if (mr_conf.containsKey("table_out")) {
			Object dbTab = mr_conf.get("table_out");
			if (dbTab instanceof String) {
				String outTabName = (String) dbTab;
				Path outPath = new Path(TABLE_PARENT + outTabName);
				FileSystem fsdl = outPath.getFileSystem(getConf());
				fsdl.delete(outPath, true);
				TableOutputFormat.setOutputPath(job, outPath);
			} else {
				Map outTab = (Map) mr_conf.get("table_out");
				if (outTab.size() != 1) {
					LOG.error("输出表只允许有一个！");
					return null;
				}
				for (Object o : outTab.entrySet()) {
					Entry entry = (Entry) o;
					String outTabName = ((String) entry.getKey()).replace('.',
							'/');
					if (!(entry.getValue() instanceof String)) {
						LOG.error("只允许指定一个输出表的分区！");
						return null;
					}
					String partStr = (String) entry.getValue();
					String expandedPartStr = expandMacroDateYmd(partStr,
							dateYmd);
					Path outPath = new Path(
							(TABLE_PARENT + outTabName + "/" + expandedPartStr));
					FileSystem fsdl = outPath.getFileSystem(getConf());
					fsdl.delete(outPath, true);
					TableOutputFormat.setOutputPath(job, outPath);
					// TableOutputFormat.addOutput(new TableInfo(outTabName,
					// expandedPartStr), job);
				}
			}
		}

		return job;
	}

	/**
	 * @param mr_conf
	 *            配置文件,若为null,默认载入当前程序目录下的mbp_mr.yaml
	 * @param args
	 *            参数配置
	 * @return
	 * @throws Exception
	 */
	public ControlledJob makeControlledJob(Map mr_conf, String[] args)
			throws Exception {
		Configuration jobConf = getConf();
		jobConf.set("date_ymd", jobConf.get("date_ymd", TEST_DATE_YMD));
		String[] otherArgs = new GenericOptionsParser(jobConf, args)
				.getRemainingArgs();
		String date_ymd = jobConf.get("date_ymd", TEST_DATE_YMD);
		LOG.info("date_ymd=" + date_ymd);
		if (mr_conf == null) {
			mr_conf = loadMrConf(MBP_MR);
		}

		// 设置 MR 本地运行环境
		jobConf.set("mapred.job.tracker", "local");
		jobConf.set("fs.default.name",
				"file:///home/yangjd/Documents/workspace/mooncloud/warehouse/");
		// System.setProperty("hadoop.home.dir",
		// "file:///D:/My Documents/Downloads/hadoop-2.3.0");
		jobConf.setBoolean(
				"mapreduce.input.fileinputformat.input.dir.recursive", true);

		Job job = makeMrJob(mr_conf, sdf.parse(date_ymd));
		if (job == null) {
			return null;
		}
		ControlledJob controlledJob = new ControlledJob(job, null);
		return controlledJob;
	}

	public MrRun() {
	}

	public MrRun(Configuration other) {
		setConf(other);
	}

	private Configuration conf;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		if (conf == null) {
			conf = new Configuration();
		}
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		Map mr_conf = loadMrConf(MBP_MR);
		ControlledJob job = makeControlledJob(mr_conf, args);
		if (job == null) {
			LOG.error("配置错误，无法构建 map/reduce 任务！");
			return 2;
		}
		int ret = job.waitForCompletion(true) ? 0 : 1;
		return ret;
	}

	public static void main(String[] args) throws Exception {
		long start = System.nanoTime();
		int ret = ToolRunner.run(new MrRun(), args);
		long end = System.nanoTime();
		LOG.info("运行时间: " + (end - start) / 1e9 + " seconds");
	}

}
