package cn.hadoop.liuyu.project;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.opencsv.CSVReader;    //解析CSV文件格式的包

/**
 * 
 * 读取文件系统中的数据
 * @function 从 map/reduce的输出结果中读取并提取数据
 * 
 */
public abstract class DataFile {

	/**
	 * @function 从 map/reduce job 的输出结果，提取key值集合
	 * @param fn HDFS上的文件路径
	 * @return list  key值的集合
	 * @throws IOException
	 */
    public static List<String> extractKeys(String fn,FileSystem fs) throws IOException {
    	FSDataInputStream in = fs.open(new Path(fn));//打开文件，作为输入流
    	List<String> retVal = new ArrayList<String>();//新建存储key值的集合list
    	////BufferedReader从字符输入流中读取文本，缓冲各个字符，从而提供字符、数组和行的高效读取
    	BufferedReader br = new BufferedReader(new InputStreamReader(in));
    	String line = br.readLine();//按行读取数据
    	while  (line != null) {//判断数据是否读取完毕
    		String[] lp = line.split("\t");  //分割读 到的行数据
    		if (lp.length > 0) {
    			retVal.add(lp[0]);//提取每行的第一个字段key
    		}
    		line = br.readLine();
    	}
    	br.close();   //关闭缓冲读取器
    	Collections.sort(retVal);//对key值进行排序
    	return retVal;
    }
    
    /**
     * @function 将 csv文件格式的每行内容转换为数组返回
     * @param 读取的一行数据
     * @return array 数组
     * @throws IOException
     */
    public static String[] getColumns(String line) throws IOException {
    	//初始化CSV格式文件读取器
		CSVReader reader = new CSVReader(new InputStreamReader(new ByteArrayInputStream(line.getBytes())));
		String[] retVal = reader.readNext();  //将 csv文件格式的每行内容转换为数组
		reader.close();
		return retVal;
	}
}
