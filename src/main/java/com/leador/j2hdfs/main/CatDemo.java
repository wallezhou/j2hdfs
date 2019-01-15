/**
 * 
 */
package com.leador.j2hdfs.main;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
* @ClassName: Demo1
* @Description: 查看hdfs上的文件内容内容
* @author: zhouwei
* @date: 2019年1月14日 下午6:49:00
*/
public class CatDemo {
	public static void main(String[] args) {
		String hdfsUri =  "hdfs://192.168.22.100:9000";
		String path = "/user/hadoop/input/hadoop-test.txt";
		cat(hdfsUri, path);		
    }
	public static void cat(String hdfsUri, String path) {
		Configuration conf = null;
		FileSystem fs = null;
		FSDataInputStream is = null;
		try {
			 // 创建Configuration对象
	        conf = new Configuration();
	        // 创建FileSystem对象
	        fs = FileSystem.get(URI.create(hdfsUri),conf);
	        // 需求：查看/user/hadoop/input/hadoop-test.txt的内容
	        // args[0] hdfs://192.168.22.100:9000/user/hadoop/input/hadoop-test.txt
	        is = fs.open(new Path(hdfsUri+path));
	        byte[] buff = new byte[1024];
	        int length = 0;
	        while((length = is.read(buff)) != -1){
	            System.out.println(new String(buff,0,length));
	        }
		} catch (IOException e) {
			System.out.println("文件流异常");
			e.printStackTrace();
		} finally {
			try {
				is.close();
				fs.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
