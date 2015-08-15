package cn.hadoop.liuyu.project;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configured;

/**
 * 
 * @function 在 MapReduce 基类中，定义基础成员变量，减少 MapReduce 主类的工作量
 * 说明项目所要提取的有用数据字段在解析后的犯罪历史数据数组中的位置
 * 
 */
public  class MapReduceJobBase extends Configured{

	/**
	 * 犯罪类型在解析后犯罪历史数据数组的下标为1的位置
	 */
	protected static final int CATEGORY_COLUMN_INDEX = 1;
	
	/**
	 * 礼拜几在解析后犯罪历史数据数组的下标为3的位置
	 */
	protected static final int DAY_OF_WEEK_COLUMN_INDEX = 3;
	
	/**
	 * 日期在解析后犯罪历史数据数组的下标为4的位置
	 */
	protected static final int DATE_COLUMN_INDEX = 4;
	
	/**
	 * 犯罪区域在解析后犯罪历史数据数组的下标为6的位置
	 */
	protected static final int DISTRICT_COLUMN_INDEX = 6;

	/**
	 * 定义日期的数据格式
	 */
	protected static final DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
	
	/**
	 * 定义 map/reduce job结果中，日期的输出格式
	 */
	protected static final DateFormat outputDateFormat = new SimpleDateFormat("yyyy/MM/dd");

	/**
	 * @function 将字符串格式的日期转换为自定义Date类型的日期
	 * @param value 包含完整的日期字符串
	 * @return Date类型的日期
	 * @throws ParseException
	 */
	protected static Date getDate(String value) throws ParseException {
		Date retVal = null;
		String[] dp = value.split(" ");
		if (dp.length > 0) {
			retVal = df.parse(dp[0]);
		}
		return retVal;
	}	
}

