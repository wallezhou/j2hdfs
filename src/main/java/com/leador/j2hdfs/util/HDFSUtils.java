/**
 * 
 */
package com.leador.j2hdfs.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
* @ClassName: HDFSUtils
* @Description: 帮助实现HDFS一些常用操作的工具类
* @author: zhouwei
* @date: 2019年1月15日 下午1:38:01
*/
public class HDFSUtils {
	public static void main(String[] args) {
		String hdfsUri = "hdfs://192.168.22.100:9000";
		String path = "/user/hadoop/test/upload_test.txt";
		String srcPath = "D:\\upload_test.txt";
		String destPath = "D:\\down_test.txt";
		FileSystem fileSystem = getFileSystem(hdfsUri);
//		System.out.println(existDir(fileSystem,path));
//		mkdir(fileSystem, path);
//		System.out.println(existDir(fileSystem,path));
		rmDirOrFile(hdfsUri, path);
	}

	/**
	* @Title: concat
	* @Description: 保证uri与path正确拼接
	* @author: zhouwei
	* @date: 2019年1月15日 下午2:31:42
	* @param hdfsUri
	* @param path  绝对路径
	* @return
	* String
	*/
	private static String concat(String hdfsUri, String path) {
		if(!path.startsWith("/")) {
			System.out.println("提醒:参数path请填写绝对路径，以/为根目录。本次操作将在您填写的参数前自动添加/。");
			System.out.println("如有错误请自行修改参数path!");
		}
		String newUri = "";
		if(hdfsUri.endsWith("/")){
			if(path.startsWith("/")) {
				path = path.substring(1, path.length());
				newUri = hdfsUri+path;
			}else{
				newUri = hdfsUri+path;
			}
		}else{
			if(path.startsWith("/")) {
				newUri = hdfsUri+path;
			}else{
				newUri = hdfsUri+"/"+path;
			}
		}
		
		return newUri;
	}
	
	/**
	* @Title: getFileSystem
	* @Description: 获取文件系统
	* @author: zhouwei
	* @date: 2019年1月15日 下午1:53:13
	* @param hdfsUri
	* @return
	* FileSystem
	*/
	public static FileSystem getFileSystem(String hdfsUri) {
        //读取配置文件
        Configuration conf = new Configuration();
        // 文件系统
        FileSystem fs = null;
        if(StringUtils.isBlank(hdfsUri)){
            // 返回默认文件系统  如果在 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            // 返回指定的文件系统,如果在本地测试，需要使用此种方法获取文件系统
            try {
                URI uri = new URI(hdfsUri.trim());
                fs = FileSystem.get(uri,conf);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return fs;
    }
	

	public static  boolean existDir(FileSystem fileSystem, String path){
        boolean flag = false;
        if (StringUtils.isEmpty(path)){
            return flag;
        }
        try{
            Path checkPath = new Path(path);
            // FileSystem对象
            if (fileSystem.isDirectory(checkPath)){
                flag = true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return flag;
    }
	
	public static  boolean existDir(String hdfsUri, String path){
        boolean flag = false;
        String newUri = concat(hdfsUri, path);
        if (StringUtils.isEmpty(newUri)){
            return flag;
        }
        try{
            Path checkPath = new Path(path);
            // FileSystem对象
            FileSystem fs = getFileSystem(hdfsUri);
            if (fs.isDirectory(checkPath)){
                flag = true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return flag;
    }
	
	public static void mkdir(String hdfsUri, String path) {
		if(!path.startsWith("/")) {
			System.out.println("本次操作无效，参数path请填写绝对路径，以/为根目录");
			return;
		}
        try {
            FileSystem fs = getFileSystem(hdfsUri);
            System.out.println("FilePath="+path);
            if(!existDir(fs, path)) {
            	// 创建目录
                fs.mkdirs(new Path(path));
            }    
            //释放资源
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
	public static void mkdir(FileSystem fileSystem, String path) {
		if(!path.startsWith("/")) {
			System.out.println("本次操作无效，参数path请填写绝对路径，以/为根目录");
			return;
		}
        try {
            System.out.println("FilePath="+path);
            if(!existDir(fileSystem, path)) {
            	// 创建目录
            	fileSystem.mkdirs(new Path(path));
            }    
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
	public static void uploadFileToHDFS(String srcPath,String hdfsUri, String destPath) {
		if(!destPath.startsWith("/")) {
			System.out.println("本次操作无效，参数destPath请填写绝对路径，以/为根目录");
			return;
		}
		try {
			FileInputStream fis=new FileInputStream(new File(srcPath));//读取本地文件
			Configuration config=new Configuration();
			FileSystem fs=FileSystem.get(URI.create(hdfsUri), config);
			OutputStream os=fs.create(new Path(destPath));
			//copy
			IOUtils.copyBytes(fis, os, 2048, true);
			System.out.println("成功上传文件 "+srcPath+" 至"+hdfsUri+destPath);
			fs.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void putFileToHDFS(String srcPath,FileSystem fileSystem, String destPath) {
		if(!destPath.startsWith("/")) {
			System.out.println("本次操作无效，参数destPath请填写绝对路径，以/为根目录");
			return;
		}
		try {
			FileInputStream fis=new FileInputStream(new File(srcPath));//读取本地文件
			Configuration config=new Configuration();
			OutputStream os=fileSystem.create(new Path(destPath));
			//copy
			IOUtils.copyBytes(fis, os, 2048, true);
			System.out.println("成功上传文件 "+srcPath+" 至"+fileSystem.getUri()+destPath);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void getFileFromHDFS(String hdfsUri, String srcPath, String destPath) {
		if(!srcPath.startsWith("/")) {
			System.out.println("本次操作无效，参数srcPath请填写绝对路径，以/为根目录");
			return;
		}
		try {
			Configuration config=new Configuration();
			//构建FileSystem
			FileSystem fs = FileSystem.get(URI.create(hdfsUri),config);
			//读取文件
			InputStream is= fs.open(new Path(srcPath));
			FileOutputStream os = new FileOutputStream(new File(destPath));
			IOUtils.copyBytes(is, os,2048, true);//保存到本地  最后 关闭输入输出流
			fs.close();
			System.out.println("成功下载 "+hdfsUri+srcPath+" 至 "+destPath);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void getFileFromHDFS(FileSystem fileSystem, String srcPath, String destPath) {
		if(!srcPath.startsWith("/")) {
			System.out.println("本次操作无效，参数srcPath请填写绝对路径，以/为根目录");
			return;
		}
		try {
			Configuration config=new Configuration();
			//构建FileSystem
			//读取文件
			InputStream is= fileSystem.open(new Path(srcPath));
			FileOutputStream os = new FileOutputStream(new File(destPath));
			IOUtils.copyBytes(is, os,2048, true);//保存到本地  最后 关闭输入输出流
			System.out.println("成功下载 "+fileSystem.getUri()+srcPath+" 至 "+destPath);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void rmDirOrFile(String hdfsUri, String path) {
		if(!path.startsWith("/")) {
			System.out.println("本次操作无效，参数path请填写绝对路径，以/为根目录");
			return;
		}
        try {
            // 返回FileSystem对象
            FileSystem fs = getFileSystem(hdfsUri);
            String fileUri = hdfsUri+path;
            // 删除文件或者文件目录  delete(Path f) 此方法已经弃用
            if( fs.delete(new Path(path),true)) {
            	System.out.println("成功删除 "+hdfsUri+path);
            }else {
            	System.out.println("文件删除失败！");
            }
            // 释放资源
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
	public static void rmDirOrFile(FileSystem fileSystem, String path) {
		if(!path.startsWith("/")) {
			System.out.println("本次操作无效，参数path请填写绝对路径，以/为根目录");
			return;
		}
        try {
            // 删除文件或者文件目录  delete(Path f) 此方法已经弃用
            if( fileSystem.delete(new Path(path),true)) {
            	System.out.println("成功删除 "+fileSystem.getUri()+path);
            }else {
            	System.out.println("文件删除失败！");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
}
