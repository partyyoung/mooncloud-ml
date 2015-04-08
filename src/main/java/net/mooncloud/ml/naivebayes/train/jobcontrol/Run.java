package net.mooncloud.ml.naivebayes.train.jobcontrol;

import java.util.Map;

import net.mooncloud.mapreduce.lib.jobcontrol.ControlledJob;
import net.mooncloud.mapreduce.lib.jobcontrol.JobControl;
import net.mooncloud.ml.naivebayes.train.targetprobability.MrRun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Run {
	private static final Log LOG = LogFactory.getLog(Run.class);

	public static void main(String[] args) throws Exception {
		long start = System.nanoTime();

		MrRun mrRun = new MrRun();
		Map mr_conf = null;
		// XXX featurefrequency
		String featurefrequency_MBP_MR = "naivebayes/train/featurefrequency/mbp_mr.yaml";
		mr_conf = MrRun.loadMrConf(featurefrequency_MBP_MR);
		ControlledJob featurefrequency_ctrlJob = mrRun.makeControlledJob(
				mr_conf, args);
		if (featurefrequency_ctrlJob == null) {
			LOG.error("配置错误，无法构建 map/reduce 任务！");
			return;
		}

		// XXX targetprobability
		String targetprobability_MBP_MR = "naivebayes/train/targetprobability/mbp_mr.yaml";
		mr_conf = MrRun.loadMrConf(targetprobability_MBP_MR);
		ControlledJob targetprobability_ctrlJob = mrRun.makeControlledJob(
				mr_conf, args);
		if (targetprobability_ctrlJob == null) {
			LOG.error("配置错误，无法构建 map/reduce 任务！");
			return;
		}

		// XXX featureprobability
		String featureprobability_MBP_MR = "naivebayes/train/featureprobability/mbp_mr.yaml";
		mr_conf = MrRun.loadMrConf(featureprobability_MBP_MR);
		ControlledJob featureprobability_ctrlJob = mrRun.makeControlledJob(
				mr_conf, args);
		if (featureprobability_ctrlJob == null) {
			LOG.error("配置错误，无法构建 map/reduce 任务！");
			return;
		}
		featureprobability_ctrlJob.addDependingJob(featurefrequency_ctrlJob);
		featureprobability_ctrlJob.addDependingJob(targetprobability_ctrlJob);

		// XXX JobControl
		JobControl jobControl = new JobControl("naivebayes.train");
		jobControl.addJob(targetprobability_ctrlJob);
		jobControl.addJob(featurefrequency_ctrlJob);
		jobControl.addJob(featureprobability_ctrlJob);

		// XXX start
		Thread theController = new Thread(jobControl);
		theController.start();
		while (!jobControl.allFinished()) {
			Thread.sleep(1000);
		}
		LOG.info("map/reduce job all finished！");
		jobControl.stop();

		long end = System.nanoTime();
		LOG.info("运行时间: " + (end - start) / 1e9 + " seconds");
	}
}
