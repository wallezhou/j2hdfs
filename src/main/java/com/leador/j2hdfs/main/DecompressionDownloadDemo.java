/**
 * 
 */
package com.leador.j2hdfs.main;

import com.leador.j2hdfs.util.HDFSUtils;

/**
* @ClassName: DecompressionDownloadDemo
* @Description: ...
* @author: zhouwei
* @date: 2019年1月24日
*/
public class DecompressionDownloadDemo {

	public static void main(String[] args) {
		String hdfsUri = "hdfs://192.168.22.100:9000";
		
		//下载tar.bz2并自动解压到locaPath1目录下，内含单文件
		String hdfsPath1 = "/user/test/hdfs_test/data_file.txt.tar.bz2";
		String localPath1 = "D:\\hdfs_test\\data_file_decompression";		
		HDFSUtils.decompressionDownload(hdfsUri, hdfsPath1, localPath1);
		
		
		////下载tar.bz2并自动解压到locaPath1目录下，内含文件夹/文件
		String hdfsPath2 = "/user/test/hdfs_test/data_folder.tar.bz2";
		String localPath2 = "D:\\hdfs_test\\data_folder_decompression";
		HDFSUtils.decompressionDownload(hdfsUri, hdfsPath2, localPath2);
		
	}
}
