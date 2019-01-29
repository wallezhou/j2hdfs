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

		
		//下载单个文件
		String localPath1 = "D:\\hdfs_test\\data_file_down.txt";
		String hdfsPath1 = "/user/test/hdfs_test/data_file.txt";
		HDFSUtils.copyFileFromHDFS(hdfsUri, hdfsPath1,localPath1);
		
		//下载文件夹
		String localPath2 = "D:\\hdfs_test\\data_folder_down";		
		String hdfsPath2 = "/user/test/hdfs_test/data_folder";
		HDFSUtils.copyFolderFromHDFS(hdfsUri, hdfsPath2, localPath2);
    }	

}
