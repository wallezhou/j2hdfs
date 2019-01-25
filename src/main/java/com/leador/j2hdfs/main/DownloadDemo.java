/**
 * 
 */
package com.leador.j2hdfs.main;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.leador.j2hdfs.util.HDFSUtils;

/**
* @ClassName: DownloadDemo
* @Description: 将HDFS中的文件拿到windows中
* @author: zhouwei
* @date: 2019年1月15日 上午9:36:30
*/
public class DownloadDemo {
	public static void main(String[] args) {
		String hdfsUri = "hdfs://192.168.22.100:9000";
		String srcPath = "/user/test/test1/";
		String destPath = "D:\\test";
		HDFSUtils.copyFolderFromHDFS(hdfsUri, srcPath, destPath);
    }	

}
