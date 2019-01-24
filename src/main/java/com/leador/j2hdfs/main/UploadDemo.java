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
		String hdfsUri = "hdfs://192.168.22.100:9000";
		String srcPath = "D:\\upload-test";
		//需要有对/user/test/的写权限
		String destPath = "/user/test/test1";
		HDFSUtils.putFolderToHDFS(srcPath, hdfsUri, destPath,true);
//		String srcPath = "D:\\test";
//		File a = new File(srcPath);
//		String[] files = a.list();
//		System.out.println(files.length);
	}
}
