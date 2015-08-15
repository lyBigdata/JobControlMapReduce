package cn.hadoop.liuyu.project;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/***
 * @function 从 MapReduce 任务中，提取数据，插入到mysql数据库
 */
public class LoadStarDB {
	private Connection db = null;//mysql数据库连接
	private Map<String, Integer> lastPrimaryKey = new HashMap<String, Integer>();
	private List<String> categories = null;//犯罪类别list
	private List<String> districts = null;//犯罪区域list
	
	//映射date主键的关系
	private final java.util.Map<Date, Integer> timeperiodLookup = new HashMap<Date, Integer>();
	private final DateFormat df = new SimpleDateFormat("MM/dd/yyyy");//插入数据库的日期格式
	private final DateFormat kdf = new SimpleDateFormat("yyyy/MM/dd");//从map/reduce任务输出文件中，解析出此日期

	/***
	 * @function 向数据库表中插入一条记录
	 * @param table 表名称
	 * @param row 包含插入字段的数据
	 * @return 返回此记录的主键id
	 * @throws SQLException
	 */
	private int insert(String table, DataRecord row) throws SQLException {
		//Statement 是 Java 执行数据库操作的一个重要方法，用于在已经建立数据库连接的基础上，向数据库发送要执行的SQL语句
		int retVal = 0;
		Statement s = db.createStatement();
		StringBuffer sql = new StringBuffer();
		sql.append("insert into ");
		sql.append(table);
		sql.append(" ");

		sql.append(row.toString());
		s.execute(sql.toString());
		if (lastPrimaryKey.containsKey(table)) {
			retVal = lastPrimaryKey.get(table) + 1;
			lastPrimaryKey.put(table, retVal);
		} else {
			lastPrimaryKey.put(table, 1);
			retVal = 1;
		}
		return retVal;
	}

	/***
	 * @function 向数据库中插入一条犯罪类别记录
	 * @param category name字段对应的值
	 * @return 返回此记录的主键id
	 * @throws SQLException
	 */
	private int insertCategory(String category) throws SQLException {
		DataRecord dr = new DataRecord();
		dr.put("name", category);
		return insert("category", dr);
	}

	/***
	 * @function 向数据库中插入一条犯罪区域记录
	 * @param district name字段对应的值 
	 * @return 返回此记录的主键id
	 * @throws SQLException
	 */
	private int insertDistrict(String district) throws SQLException {
		DataRecord dr = new DataRecord();
		dr.put("name", district);
		return insert("district", dr);
	}

	/***
	 * @function 将日期date拆分为字段 year, month, week, 和 day
	 * @param dr 包含date被拆分的字段
	 * @param d 需要拆分的date日期
	 */
	private void setTimePeriod(DataRecord dr, Date d) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		dr.put("year", cal.get(Calendar.YEAR));
		dr.put("month", cal.get(Calendar.MONTH));
		dr.put("week", cal.get(Calendar.WEEK_OF_MONTH));
		dr.put("day", cal.get(Calendar.DAY_OF_MONTH));
	}

	/***
	 * @function 如果日期date已经存在表中，返回主键id，如果不存在，则插入数据库并返回主键id
	 * @param d 日期date
	 * @return 返回此日期对应的主键id
	 * @throws SQLException
	 */
	private int insertTimePeriod(Date d) throws SQLException {
		int retVal = 0;
		if (timeperiodLookup.containsKey(d)) {
			retVal = timeperiodLookup.get(d);
		} else {
			DataRecord dr = new DataRecord();
			setTimePeriod(dr, d);
			retVal = insert("timeperiod", dr);
			timeperiodLookup.put(d, retVal);
		}
		return retVal;
	}

	/***
	 * @function 将数据记录插入fact表中
	 * @param districtId 犯罪区域外键id
	 * @param categoryId 犯罪类别外键id
	 * @param timeId 日期外键id
	 * @param crimes 在某一日期  某一区域 发生某一犯罪类别的总犯罪次数
	 * committed in this district of this category at his time* 
	 * @throws SQLException
	 */
	private void insertFact(int districtId, int categoryId, int timeId,
			int crimes) throws SQLException {
		DataRecord dr = new DataRecord();
		dr.put("district_id", districtId);
		dr.put("category_id", categoryId);
		dr.put("time_id", timeId);
		dr.put("crimes", crimes);
		insert("fact", dr);
	}

	/***
	 * @function  从SanFrancisco Crime map/reduce job输出结果中，读取数据
	 * @param categoryReport 犯罪类别文件路径
	 * @param districtReport 犯罪区域文件路径
	 * @throws IOException* 
	 * @throws SQLException
	 */
	private void setup(String categoryReport, String districtReport,FileSystem fs)
			throws IOException, SQLException {
		categories = DataFile.extractKeys(categoryReport,fs);
		districts = DataFile.extractKeys(districtReport,fs);
		for (String category : categories) {
			insertCategory(category);
		}
		for (String district : districts) {
			insertDistrict(district);
		}
	}

	/***
	 * @function 清空name表中的所有记录
	 * @param name 表名称
	 * @throws SQLException
	 */
	private void truncate(String name) throws SQLException {
		Statement s = db.createStatement();
		s.execute("truncate table ".concat(name));
		s.close();
	}

	/***
	 *  @function 调用truncate()方法，清空表记录
	 *  @throws SQLException
	 */
	private void reset() throws SQLException {
		truncate("fact");
		truncate("category");
		truncate("district");
		truncate("timeperiod");
	}

	/***
	 * @function 解析加载的数据
	 * @param categoryReport 犯罪类别文件路径
	 * @param districtReport 犯罪区域文件路径
	 * @param dbhost 数据库地址
	 * @param dbname 数据库名称
	 * @param dbuser 用户名
	 * @param dbpassword 密码
	 * @throws ClassNotFoundException* 
	 * @throws SQLException* 
	 * @throws IOException
	 */
	public LoadStarDB(String categoryReport, String districtReport,
			String dbhost, String dbname, String dbuser, String dbpassword,FileSystem fs)
			throws ClassNotFoundException, SQLException, IOException {
		Class.forName("com.mysql.jdbc.Driver");
		String cs = MessageFormat
				.format("jdbc:mysql://192.168.138.128:3306/HadoopTest?user=root&password=12035318&autoReconnect=true",
						new Object[] { dbhost, dbname, dbuser, dbpassword });
		db = DriverManager.getConnection(cs);
		reset();
		setup(categoryReport, districtReport,fs);
	}

	/***
	 * 
	 * @function 处理 SanFranciscoCrimPrepOlap map/reduce job任务输出结果，填充 timeperiod表和fact表  
	 * @param dataFile 文件路径
	 * @throws IOException* 
	 * @throws ParseException
	 */
	private void processData(String dataFile,FileSystem fs) throws IOException,
			ParseException {
		FSDataInputStream in = fs.open(new Path(dataFile));//打开数据流
		BufferedReader br = new BufferedReader(new InputStreamReader(in));//读取数据
		String line = br.readLine();
		while (line != null) {
			String[] lp = line.split("\t");
			if (lp.length > 0) {
				Date d = kdf.parse(lp[0]);//日期
				String[] data = DataFile.getColumns(lp[1]);
				if (data.length == 3) {
					try {
						int categoryId = Integer.parseInt(data[0]) + 1;//犯罪类别id
						int districtId = Integer.parseInt(data[1]) + 1;//犯罪区域id
						int crimes = Integer.parseInt(data[2]);//犯罪次数
						int timeId = insertTimePeriod(d);//时间id
						insertFact(districtId, categoryId, timeId, crimes);//插入fact表
					} catch (NumberFormatException nfe) {
						System.err.println("invalid data: " + line);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				} else {
					System.err.println("invalid data: " + line);
				}
			}
			line = br.readLine();
		}
		br.close();
	}

	/*** 
	 * @function 运行job任务
	 * @param args 
	 * @throws IOException 
	 * */
	public static void main(String[] args) throws IOException {
		String[] args0 = {
                "hdfs://master:9000/middle/crime/out1/part-r-00000",
                "hdfs://master:9000/middle/crime/out2/part-r-00000",
                "hdfs://master:9000/middle/crime/out3/part-r-00000",
                "192.168.138.128:3306",
                "HadoopTest",
                "root",
                "12035318"};
		if (args0.length == 7) {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create("hdfs://master:9000"), conf);
			try {
				LoadStarDB m = new LoadStarDB(args0[0], args0[1], args0[3],args0[4], args0[5], args0[6],fs);
				m.processData(args0[2],fs);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		} else {
			System.err.println("\nusage: java -jar sfcrime.hadoop.mapreduce.jobs-0.0.1-SNAPSHOT.jar com.dynamicalsoftware.olap.etl.LoadStarDB path/to/category/report path/to/district/report path/to/star/data dbhost dbname dbuser dbpassword\n");
		}
	}

	/*** 
	 * 生成一条数据记录
	 */
	
	public class DataRecord extends HashMap<String, Object> {
		@Override
		public String toString() {
			StringBuffer retVal = new StringBuffer();
			// 生成表的数据字段
			retVal.append("(");
			boolean first = true;
			for (String key : keySet()) {
				if (first) {
					first = false;
				} else {
					retVal.append(",");
				}
				retVal.append(key);
			}
			//生成表字段对应的值
			retVal.append(") values (");
			first = true;
			for (String key : keySet()) {
				Object o = get(key);
				if (first) {
					first = false;
				} else {
					retVal.append(",");
				}
				if (o instanceof Long) {
					retVal.append(((Long) o).toString());
				} else if (o instanceof Integer) {
					retVal.append(((Integer) o).toString());
				} else if (o instanceof Date) {
					Date d = (Date) o;
					retVal.append("'");
					retVal.append(df.format(d));
					retVal.append("'");
				} else if (o instanceof String) {
					retVal.append("'");
					retVal.append(o.toString());
					retVal.append("'");
				}
			}
			retVal.append(")");
			//返回一条sql格式的数据记录
			return retVal.toString();
		}
	}
}

