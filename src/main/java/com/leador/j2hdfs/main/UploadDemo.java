/**
 * 
 */
package com.leador.j2hdfs.main;

import java.io.File;

import com.leador.j2hdfs.util.HDFSUtils;

/**
* @ClassName:
* @Description: 上传本地文件到hdfs
* @author: zhouwei
* @date: 2019年1月15日 上午11:22:32
*/
public class UploadDemo {
	public static void main(String[] args) {
		//需要有对/user/test/的写权限
		String hdfsUri = "hdfs://192.168.22.100:9000";
		//上传当个文件
		String localPath1 = "D:\\hdfs_test\\data_file.txt";
		String hdfsPath1 = "/user/test/hdfs_test/data_file.txt";
		HDFSUtils.putFileToHDFS(localPath1, hdfsUri, hdfsPath1);
		
		//上传文件夹
		String localPath2 = "D:\\hdfs_test\\data_folder";		
		String hdfsPath2 = "/user/test/hdfs_test/data_folder";
		HDFSUtils.putFolderToHDFS(localPath2, hdfsUri, hdfsPath2);

	}
}
