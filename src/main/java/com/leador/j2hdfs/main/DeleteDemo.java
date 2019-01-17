/**
 * 
 */
package com.leador.j2hdfs.main;

import com.leador.j2hdfs.util.HDFSUtils;

/**
* @ClassName: DeleteDemo
* @Description: ...
* @author: zhouwei
* @date: 2019年1月16日 下午4:58:22
*/
public class DeleteDemo {
	public static void main(String[] args) {
		String hdfsUri = "hdfs://192.168.22.100:9000";
		String destPath = "/user/hadoop/upload_folder_test/";
		HDFSUtils.rmDirOrFile(hdfsUri, destPath);
	}
}
