/**
 * 
 */
package com.leador.j2hdfs.main;

import com.leador.j2hdfs.util.HDFSUtils;

/**
* @ClassName: CompressedUploadDemo
* @Description: ...
* @author: zhouwei
* @date: 2019年1月24日
*/
public class CompressedUploadDemo {
	public static void main(String[] args) {
		String hdfsUri = "hdfs://192.168.22.100:9000";
		String srcPath = "D:\\upload_test.txt";
		//需要有对/user/test/的写权限
		String destPath = "/user/test/upload_test.txt.bz2";
		HDFSUtils.compressedUpload(srcPath, hdfsUri, destPath);
	}
}
