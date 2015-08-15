package cn.hadoop.liuyu.project;

import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.text.ParseException;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * 
 * @function 统计每天每种犯罪类型在每个区域发生的次数
 *
 */
public class SanFranciscoCrimePrepOlap extends MapReduceJobBase implements Tool {

	private static Logger log = Logger.getLogger(SanFranciscoCrimePrepOlap.class.getCanonicalName());
	private static List<String> categories = null;
	private static List<String> districts = null;
	private static final java.util.Map<String, Integer> categoryLookup = new HashMap<String, Integer>();
	private static final java.util.Map<String, Integer> districtLookup = new HashMap<String, Integer>();
	public static abstract class Map extends Mapper<LongWritable, Text, Text, Text> {
		protected int keyID = 0;
		protected int valueID = 0;
		protected int value2ID = 0;
		
		/**
		 * @function 将key值转换为规范的数据格式
		 * @param value 包含不规范的 key值
		 * @return 返回规范的key值
		 * @throws ParseException
		 */
		protected abstract String formatKey(String value) throws ParseException;
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			try {
				String[] col = DataFile.getColumns(line);//将读取的每行数据转换为数组
				if (col != null) {
					if (col.length >= (DISTRICT_COLUMN_INDEX + 1)) {
						Text tk = new Text();
						tk.set(formatKey(col[keyID]));//将日期作为key值
						Text tv = new Text();
						StringBuffer sv = new StringBuffer();
						sv.append("\"");
						sv.append(col[valueID]);//犯罪区域
						sv.append("\"");
						sv.append(",");
						sv.append("\"");
						sv.append(col[value2ID]);//犯罪类型
						sv.append("\"");
						tv.set(sv.toString());
						context.write(tk, tv);
					} else {
						log.warning(MessageFormat.format("Data {0} did not parse into columns.", new Object[]{line}));
					}
				} else {
					log.warning(MessageFormat.format("Data {0} did not parse into columns.", new Object[]{line}));
				}
			} catch (NumberFormatException nfe) {
				log.log(Level.WARNING, MessageFormat.format("Expected {0} to be a number.\n", new Object[]{line}), nfe);
			} catch (IOException e) {
				log.log(Level.WARNING, MessageFormat.format("Cannot parse {0} into columns.\n", new Object[]{line}), e);
			} catch (ParseException e) {
				log.log(Level.WARNING, MessageFormat.format("Expected {0} to be a date but it was not.\n", new Object[]{line}), e);
			}
		}
	}
	
	/**
	 * @function 将 map 输入数据的日期作为key，犯罪区域和犯罪类型作为value，然后输出
	 */
	public static class DateMapByCategoryAndDistrict extends Map {
		public DateMapByCategoryAndDistrict() {
			keyID = DATE_COLUMN_INDEX;//代表日期下标
			valueID = DISTRICT_COLUMN_INDEX;//代表犯罪区域下标
			value2ID = CATEGORY_COLUMN_INDEX;//代表犯罪类型下标
		}

		@Override
		protected String formatKey(String value) throws ParseException {
			return outputDateFormat.format(getDate(value));
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
						// 分配和初始化犯罪类型所在区域的二维数组
						int[][] crimes = new int[categories.size()][districts.size()];
						for (int i = 0; i < categories.size(); i++) {
							for (int j = 0; j < districts.size(); j++) {
								crimes[i][j] = 0;
							}
						}
						//统计犯罪类型/区域二维数组的值（即每种犯罪类型在每个区域发生的次数）
						for (Text crime:values) {
							String[] cols = DataFile.getColumns(crime.toString());
							if (cols.length == 2) {
								if (categoryLookup.containsKey(cols[1])) {
									if (districtLookup.containsKey(cols[0])) {
										int cat = categoryLookup.get(cols[1]);
										int dist = districtLookup.get(cols[0]);
										crimes[cat][dist]++;
									} else {
										log.warning(MessageFormat.format("District {0} not found.", new Object[]{cols[0]}));
									}
								} else {
									log.warning(MessageFormat.format("Category {0} not found.", new Object[]{cols[1]}));
								}
							} else {
								log.warning(MessageFormat.format("Input {0} was in unexpected format", new Object[]{crime}));
							}
						}
						//将非0二维数组的犯罪类别下标，犯罪区域下标，犯罪次数作为value输出
						for (int i = 0; i < categories.size(); i++) {
							for (int j = 0; j < districts.size(); j++) {
								if (crimes[i][j] > 0) {
									StringBuffer sv = new StringBuffer();
									sv.append(new Integer(i).toString());//犯罪类别下标
									sv.append(",");
									sv.append(new Integer(j).toString());//犯罪区域下标
									sv.append(",");
									sv.append(new Integer(crimes[i][j]));//犯罪次数
									Text tv = new Text();
									tv.set(sv.toString());
									context.write(key, tv);
								}
							}
						}
		}
	}
	/**
	 * @function 加载已经生成的 犯罪类别数据和犯罪区域数据，并将这些数据排序后存入Map
	 * @param categoryReport SanFranciscoCrime job任务输出犯罪类别的文件路径
	 * @param districtReport SanFranciscoCrime job任务输出犯罪区域的文件路径
	 * @throws IOException
	 */
	private static void  setup(String categoryReport, String districtReport,FileSystem fs) throws IOException {
		categories = DataFile.extractKeys(categoryReport,fs);
		districts = DataFile.extractKeys(districtReport,fs);
		int i = 0;
		for (String category : categories) {
			categoryLookup.put(category, i++);
		}
		i = 0;
		for (String district : districts) {
			districtLookup.put(district, i++);
		}
	}

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

		Path out = new Path(arg0[3]);
		
		FileSystem hdfs = out.getFileSystem(conf);
		if (hdfs.isDirectory(out)) {
			hdfs.delete(out, true);
		}
		
		// 任务1
		Job job = Job.getInstance(conf);
		job.setJarByClass(SanFranciscoCrimePrepOlap.class);

		job.setMapperClass(DateMapByCategoryAndDistrict.class);//Mapper
		job.setReducerClass(Reduce.class);//Reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[3]));
		job.waitForCompletion(true);//提交任务
		return 0;
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 4) {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create("hdfs://master:9000"), conf);
			//调用setup
			setup(args[1], args[2],fs);
			//执行MapReduce任务
			int ec = ToolRunner.run(conf, new SanFranciscoCrimePrepOlap(), args);
			System.exit(ec);
		} else {
			System.err.println("\nusage: bin/hadoop jar sfcrime.hadoop.mapreduce.jobs-0.0.1-SNAPSHOT.jar SanFranciscoCrimePrepOlap path/to/category/report path/to/district/report path/to/input/data path/to/output/data");
		}
	}	
}

