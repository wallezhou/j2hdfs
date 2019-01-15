/**
 * 
 */
package com.leador.j2hdfs.main;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
* @ClassName: DownloadDemo
* @Description: 将HDFS中的文件拿到windows中
* @author: zhouwei
* @date: 2019年1月15日 上午9:36:30
*/
public class DownloadDemo {
	public static void main(String[] args) {
		String hdfsUri = "hdfs://192.168.22.100:9000";
		String path = "/user/hadoop/input/hadoop-test.txt";
		String localPath = "D://我是从hdfs下载下来的.txt";
		downFile(hdfsUri, path, localPath);
    }	
	
	public static void downFile(String hdfsUri, String path,String localPath) {
		Configuration conf = null;
		FileSystem fs = null;
		FSDataInputStream is = null;
		FileOutputStream os = null;
		try {			
			 // 创建Configuration对象
	        conf = new Configuration();
	        // 创建FileSystem对象
	        fs = FileSystem.get(URI.create(hdfsUri),conf);
	        // 需求：下载/user/hadoop/input/hadoop-test.txt到windows主机
	        is = fs.open(new Path(hdfsUri+path));
	        os = new FileOutputStream(localPath);
//	        byte[] buff = new byte[1024];
//	        int length = 0;
//	        while((length = is.read(buff)) != -1){
//	        	os.write(buff,0,length);
//	            os.flush();
//	        }
	        IOUtils.copyBytes(is, os, 1024, true);
		} catch (IOException e) {
			System.out.println("文件流异常");
			e.printStackTrace();
		} finally {
			try {
				os.close();
				is.close();
				fs.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
