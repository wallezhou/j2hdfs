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
		String srcPath = "/user/test/upload_test.txt.bz2";
//		String destPath = "D:\\我是从hdfs下载下来的.txt";
//		HDFSUtils.copyFileFromHDFS(hdfsUri, srcPath, destPath);
		String destPath = "D:\\upload_test2.txt";
		HDFSUtils.decompressionDownload(hdfsUri, srcPath, destPath);
	}
}
