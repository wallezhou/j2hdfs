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

import com.leador.j2hdfs.util.HDFSUtils;

/**
* @ClassName: Demo1
* @Description: 查看hdfs上的文件内容内容
* @author: zhouwei
* @date: 2019年1月14日 下午6:49:00
*/
public class CatDemo {
	public static void main(String[] args) {
		String hdfsUri =  "hdfs://192.168.22.100:9000";
		String filePath = "/user/test/hdfs_test/data_file.txt";
		HDFSUtils.readFile(hdfsUri, filePath);		
    }
}
