package cn.hadoop.liuyu.project;

import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @function 统计每个事件在每个周时段内发生的次数
 * 分别统计出不同犯罪类别在周时段内发生的次数和不同区域在周时段内发生犯罪的次数
 *
 */
public class SanFranciscoCrime extends MapReduceJobBase  implements Tool {

	private static Logger log = Logger
			.getLogger(SanFranciscoCrime.class.getCanonicalName());

	/**
	 * CrimeMapper是一个公共的父类
	 */
	public static class CrimeMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		protected int keyID = 0;

		protected int valueID = 0;

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			try { 
				String[] col = DataFile.getColumns(line); //将 csv文件格式的每行内容转换为数组返回
				if (col != null) {
					// 防止数组超界
					if (col.length >= (DISTRICT_COLUMN_INDEX + 1)) {
						//过滤文件第一行头部名称
						if (!"date".equalsIgnoreCase(col[valueID])) {
							Text tk = new Text();
							tk.set(col[keyID]);
							Text tv = new Text();
							tv.set(col[valueID]);
							context.write(tk, tv);
						}
					} else {
						log.warning(MessageFormat.format(
								"Data {0} did not parse into columns.",
								new Object[] { line }));
					}
				} else {
					log.warning(MessageFormat.format(
							"Data {0} did not parse into columns.",
							new Object[] { line }));
				}
			} catch (NumberFormatException nfe) {
				log.log(Level.WARNING, MessageFormat
						.format("Expected {0} to be a number.\n",
								new Object[] { line }), nfe);
			} catch (IOException e) {
				log.log(Level.WARNING, MessageFormat.format(
						"Cannot parse {0} into columns.\n",
						new Object[] { line }), e);
			}
		}
	}

	/**
	 * 输出key为犯罪类别，value为日期
	 */
	public static class CategoryMapByDate extends CrimeMapper {
		public CategoryMapByDate() {
			keyID = CATEGORY_COLUMN_INDEX;//key为犯罪类别
			valueID = DATE_COLUMN_INDEX;//value为日期
		}
	}

	/**
	 *  输出key为犯罪区域，value为日期
	 */
	public static class DistrictMapByDate extends CrimeMapper {
		public DistrictMapByDate() {
			keyID = DISTRICT_COLUMN_INDEX;//key为犯罪区域
			valueID = DATE_COLUMN_INDEX;//value为日期
		}
	}

	/**
	 * 统计并解析 Mapper 端的输出结果
	 */
	public static class CrimeReducerByWeek extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			List<String> incidents = new ArrayList<String>();
			// 将values放入incidents列表中
			for (Text value : values) {
				incidents.add(value.toString());
			}
			if (incidents.size() > 0) {
				//对incidents列表排序
				Collections.sort(incidents);
				java.util.Map<Integer, Integer> weekSummary = new HashMap<Integer, Integer>();
				//因为是对1-3月数据分析，周时段（weekly buckets）最大为15，所以weekSummary长度为15即可
				for (int i = 0; i < 16; i++) {
					weekSummary.put(i, 0);
				}
				//统计每个周时段（weekly buckets）内，该事件发生的次数
				for (String incidentDay : incidents) {
					try {
						Date d = getDate(incidentDay);
						Calendar cal = Calendar.getInstance();
						cal.setTime(d);
						int week = cal.get(Calendar.WEEK_OF_MONTH);//这个月的第几周
						int month = cal.get(Calendar.MONTH);//第几个月，从0开始
						//如果累积的时间是以周为时间单位，此系统就称为周时段（weekly buckets）。
						//周时段的计算公式，最大为15，它只是一种统计方式，不必深究
						int bucket = (month * 5) + week;
						//统计每个周时段内，该事件发生的次数
						if (weekSummary.containsKey(bucket)) {
							weekSummary.put(bucket, new Integer(weekSummary
									.get(bucket).intValue() + 1));
						} else {
							weekSummary.put(bucket, new Integer(1));
						}
					} catch (ParseException pe) {
						log.warning(MessageFormat.format("Invalid date {0}",
								new Object[] { incidentDay }));
					}
				}
				// 将该事件在每个周时段内发生的次数生成字符串输出
				StringBuffer rpt = new StringBuffer();
				boolean first = true;
				for (int week : weekSummary.keySet()) {
					if (first) {
						first = false;
					} else {
						rpt.append(",");
					}
					rpt.append(new Integer(weekSummary.get(week)).toString());
				}
				String list = rpt.toString();
				Text tv = new Text();
				tv.set(list);
				//value为0-15周时段内，该事件发生的次数
				context.write(key, tv);
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		// 任务1
		Configuration conf1 = new Configuration();
		Path out1 = new Path(args[1]);
		FileSystem hdfs1 = out1.getFileSystem(conf1);
		if (hdfs1.isDirectory(out1)) {
			hdfs1.delete(out1, true);
		}
		Job job1 =  Job.getInstance(conf1);
		job1.setJarByClass(SanFranciscoCrime.class);

		job1.setMapperClass(CategoryMapByDate.class);
		job1.setReducerClass(CrimeReducerByWeek.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		// 任务2
		Configuration conf2 = new Configuration();
		Path out2 = new Path(args[2]);
		FileSystem hdfs2 = out2.getFileSystem(conf2);
		if (hdfs2.isDirectory(out2)) {
			hdfs2.delete(out2, true);
		}
		Job job2 =  Job.getInstance(conf2);
		job2.setJarByClass(SanFranciscoCrime.class);

		job2.setMapperClass(DistrictMapByDate.class);
		job2.setReducerClass(CrimeReducerByWeek.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		// 构造一个 cJob1
		ControlledJob cJob1 = new ControlledJob(conf1);
		//设置 MapReduce job1
		cJob1.setJob(job1);

		// 构造一个 cJob2
		ControlledJob cJob2 = new ControlledJob(conf2);
		//设置 MapReduce job2
		cJob2.setJob(job2);
		
		//cJob2.addDependingJob(cJob1);// cjob2依赖cjob1
		
		// 定义job管理对象
		JobControl jobControl = new JobControl("12");

		//把两个构造的job加入到JobControl中
		jobControl.addJob(cJob1);
		jobControl.addJob(cJob2);

		//启动线程运行任务
		Thread t = new Thread(jobControl);
		t.start();
		while (true) {
			if (jobControl.allFinished()) {
				jobControl.stop();
				break;
			}

		}
		return 0;

	}

	public static void main(String[] args) throws Exception {
		String[] args0 = {
                "hdfs://master:9000/middle/crime/crime.csv",
                "hdfs://master:9000/middle/test/out1/",
                "hdfs://master:9000/middle/test/out2/" };
		int ec = ToolRunner.run(new Configuration(), new SanFranciscoCrime(), args0);
		System.exit(ec);
	}
}

