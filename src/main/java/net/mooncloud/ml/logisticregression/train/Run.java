package net.mooncloud.ml.logisticregression.train;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import net.mooncloud.mapreduce.lib.jobcontrol.ControlledJob;
import net.mooncloud.util.Yaml;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Run
{
	private static final Log LOG = LogFactory.getLog(Run.class);

	static Yaml yaml = new Yaml();

	public static void main(String[] args) throws Exception
	{
		long start = System.nanoTime();

		int D = 10, ITERATIONS = 200;
		Random rand = new Random(42);
		// Initialize w to a random value
		String[] w = new String[D + 1];
		for (int i = 0; i < D + 1; i++)
		{
			w[i] = String.valueOf(2 * rand.nextDouble() - 1);
		}
		System.out.print("Final w: ");
		System.out.println(Arrays.toString(w));

		MrRun mrRun = new MrRun();
		Map mr_conf = null;

		double E = 0.0;
		for (int i = 1; i <= ITERATIONS; i++)
		{
			// XXX error
			String error_MBP_MR = "logisticregression/error/mbp_mr.yaml";
			mr_conf = MrRun.loadMrConf(error_MBP_MR);
			ControlledJob error_ctrlJob = mrRun.makeControlledJob(mr_conf, args);
			Configuration error_conf = error_ctrlJob.getJob().getConfiguration();
			error_conf.setStrings("weights", w);
			if (error_ctrlJob == null)
			{
				LOG.error("配置错误，无法构建 map/reduce 任务！");
				return;
			}
			error_ctrlJob.waitForCompletion(true);

			// TODO 获取误差
			Path error_output = new Path(error_conf.get("mapred.output.dir"));
			FileSystem error_hdfs = error_output.getFileSystem(error_conf);
			FileStatus[] error_fstatus = error_hdfs.listStatus(error_output);
			FSDataInputStream in = null;
			try
			{
				for (FileStatus fileStatus : error_fstatus)
				{
					Path p = fileStatus.getPath();
					if (!error_hdfs.isFile(p) || p.getName().startsWith("_"))
						continue;
					in = error_hdfs.open(p);
					String line;
					if ((line = in.readLine()) != null)
					{
						E = Double.parseDouble(line);
					}
				}

			}
			finally
			{
				try
				{
					in.close();
				}
				catch (Exception e)
				{
				}
			}
			if (E < 1e-3)
			{
				break;
			}

			System.out.println("On iteration " + i);

			// XXX gradient
			String gradient_MBP_MR = "logisticregression/gradient/mbp_mr.yaml";
			mr_conf = MrRun.loadMrConf(gradient_MBP_MR);
			ControlledJob gradient_ctrlJob = mrRun.makeControlledJob(mr_conf, args);
			Configuration gradient_conf = gradient_ctrlJob.getJob().getConfiguration();
			gradient_conf.setStrings("weights", w);
			if (gradient_ctrlJob == null)
			{
				LOG.error("配置错误，无法构建 map/reduce 任务！");
				return;
			}
			gradient_ctrlJob.waitForCompletion(true);

			// TODO 获取权值
			Path gradient_output = new Path(gradient_conf.get("mapred.output.dir"));
			FileSystem gradient_hdfs = gradient_output.getFileSystem(gradient_conf);
			FileStatus[] gradient_fstatus = gradient_hdfs.listStatus(gradient_output);
			try
			{
				for (FileStatus fileStatus : gradient_fstatus)
				{
					Path p = fileStatus.getPath();
					if (!gradient_hdfs.isFile(p) || p.getName().startsWith("_"))
						continue;
					in = gradient_hdfs.open(p);
					String line;
					if ((line = in.readLine()) != null)
					{
						ArrayList<Object> ww = ((ArrayList<Object>) yaml.load(line));
						for (int k = 0; k < D + 1; k++)
						{
							w[k] = ww.get(k).toString();
						}
					}
				}

			}
			finally
			{
				try
				{
					in.close();
				}
				catch (Exception e)
				{
				}
			}
		}

		// gradient_ctrlJob.addDependingJob(error_ctrlJob);
		// // XXX JobControl
		// JobControl jobControl = new JobControl("logistic_regression.train");
		// jobControl.addJob(error_ctrlJob);
		// jobControl.addJob(gradient_ctrlJob);
		//
		// // XXX start
		// Thread theController = new Thread(jobControl);
		// theController.start();
		// while (!jobControl.allFinished())
		// {
		// Thread.sleep(1000);
		// }
		// LOG.info("map/reduce job all finished！");
		// jobControl.stop();

		System.out.println("Final E: " + E);
		System.out.print("Final w: ");
		System.out.println(Arrays.toString(w));
		long end = System.nanoTime();
		LOG.info("运行时间: " + (end - start) / 1e9 + " seconds");
	}
}
