/**
 * 
 */
package com.walle.j2hdfs.main;

import com.walle.j2hdfs.util.HDFSUtils;

/**
* @ClassName: CompressedUploadDemo
* @Description: ...
* @author: zhouwei
* @date: 2019年1月24日
*/
public class CompressedUploadDemo {
	public static void main(String[] args) {
		//需要有对/user/test/的写权限
		String hdfsUri = "hdfs://192.168.22.100:9000";
		//单个文件打包压缩为tar.bz2上传到hdfs
		String localPath1 = "D:\\hdfs_test\\data_file.txt";
		String hdfsPath1 = "/user/test/hdfs_test/data_file.txt.tar.bz2";
		HDFSUtils.compressedUpload(localPath1, hdfsUri, hdfsPath1);
		
		//文件夹打包压缩为tar.bz2上传到hdfs
		String localPath2 = "D:\\hdfs_test\\data_folder";		
		String hdfsPath2 = "/user/test/hdfs_test/data_folder.tar.bz2";
		HDFSUtils.compressedUpload(localPath2, hdfsUri, hdfsPath2);
	}
			
}
