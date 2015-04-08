package net.mooncloud.ml.roughset.train.featureselect;

import java.util.HashMap;
import java.util.Map;

import net.mooncloud.mapreduce.lib.jobcontrol.ControlledJob;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class Run {

	private static final Log LOG = LogFactory.getLog(Run.class);

	public static void main(String[] args) throws Exception {
		long start = System.nanoTime();

		MrRun mrRun = new MrRun();
		Map mr_conf = null;
		// XXX featurefrequency
		String featureselect_MBP_MR = "roughset/train/featureselect/mbp_mr.yaml";
		mr_conf = MrRun.loadMrConf(featureselect_MBP_MR);
		ControlledJob featureselect_ctrlJob = mrRun.makeControlledJob(mr_conf,
				args);
		if (featureselect_ctrlJob == null) {
			LOG.error("配置错误，无法构建 map/reduce 任务！");
			return;
		}

		// 配置全局参数
		Configuration conf = featureselect_ctrlJob.getJob().getConfiguration();
		String output_dir = conf.get("mapred.output.dir");
		if (mr_conf.containsKey("feature_evaluate")) {// 特征编号,用","连接成字符串
			conf.set("feature_evaluate", mr_conf.get("feature_evaluate")
					.toString());
		}
		if (mr_conf.containsKey("feature_left")) { // 待评价特征编号,用","连接成字符串
			conf.set("feature_left", mr_conf.get("feature_left").toString());
		}
		if (mr_conf.containsKey("feature_select")) {// 已选择特征编号,用","连接成字符串
			conf.set("feature_select", mr_conf.get("feature_select").toString());
		}
		if (mr_conf.containsKey("target_number")) {// 目标特征编号
			conf.set("target_number", mr_conf.get("target_number").toString());
		}
		long preSig = 0;
		HashMap<Long, Long> featureSig = new HashMap<Long, Long>();

		do {
			featureselect_ctrlJob.waitForCompletion(true);

			// TODO 获取重要度最高的特征
			Path outputPath = new Path(output_dir);
			FileSystem hdfs = outputPath.getFileSystem(conf);
			FileStatus[] fstatus = hdfs.listStatus(outputPath);
			FSDataInputStream in = null;
			long maxSig = 0, maxFeature = 0;
			try {
				for (int i = 0; i < fstatus.length; i++) {
					Path p = fstatus[i].getPath();
					if (!hdfs.isFile(p) || p.getName().startsWith("_"))
						continue;
					in = hdfs.open(p);
					String line;
					while ((line = in.readLine()) != null) {
						String[] fc = line.split(",");
						int sig = Integer.parseInt(fc[1]);
						if (maxSig < sig) {
							maxSig = sig;
							maxFeature = Integer.parseInt(fc[0]);
						}
					}
				}

			} finally {
				try {
					in.close();
				} catch (Exception e) {
				}
			}

			if (preSig < maxSig) {
				// TODO 更新特征选择
				String feature_select = conf.get("feature_select");
				conf.set("feature_select",
						feature_select == null ? String.valueOf(maxFeature)
								: feature_select + "," + maxFeature);
				preSig = maxSig;
				featureSig.put(maxFeature, maxSig);

				LOG.error("feature_select: " + featureSig);
				LOG.error("feature_select: " + conf.get("feature_select"));

				Thread.sleep(2 * 1000);

				// TODO 评价剩余的属性
				Job job = new Job(conf);
				featureselect_ctrlJob = new ControlledJob(job, null);
				hdfs.delete(outputPath, true);

			} else {
				LOG.error("feature_select: " + featureSig);
				LOG.error("feature_select: " + conf.get("feature_select"));
				break;
			}
		} while (true);

		long end = System.nanoTime();
		LOG.info("运行时间: " + (end - start) / 1e9 + " seconds");
	}
}
