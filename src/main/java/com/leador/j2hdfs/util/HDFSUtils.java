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
import java.util.Comparator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
//	public static void main(String[] args) {
//		String hdfsUri = "hdfs://192.168.22.100:9000/";
////		String path = "/user/hadoop/test/upload_test.txt";
////		String srcPath = "D:\\upload_test.txt";
////		String destPath = "D:\\down_test.txt";
////		FileSystem fileSystem = getFileSystem(hdfsUri);
////		System.out.println(existDir(fileSystem,path));
////		mkdir(fileSystem, path);
////		System.out.println(existDir(fileSystem,path));
////		rmDirOrFile(hdfsUri, path);
//
//	}

	
	/**
	* @Title: isAbsolutePath
	* @Description:检查输入的路径是否绝对路径
	* @author: zhouwei
	* @date: 2019年1月16日 上午9:33:49
	* @param path 文件路径
	* @return
	* boolean
	*/
	private static boolean isAbsolutePath(String path) {
		if(!path.startsWith("/")) {
			System.out.println("参数"+path+"请填写绝对路径，以/为根目录");
			return false;
		}
		return true;
	}
	
	/**
	* @Title: concat
	* @Description: 拼接两条路径，返回拼接的结果
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param path1
	* @param path2
	* @return
	* String
	*/
	private static String concat(String path1, String path2) {
		String newUri = "";
		if(path1.endsWith("/")){
			if(path2.startsWith("/")) {
				path2 = path2.substring(1, path2.length());
				newUri = path1+path2;
			}else{
				newUri = path1+path2;
			}
		}else{
			if(path2.startsWith("/")) {
				newUri = path1+path2;
			}else{
				newUri = path1+"/"+path2;
			}
		}
		
		return newUri;
	}
	
	/**
	* @Title: getFileSystem
	* @Description: 获取文件系统
	* @author: zhouwei
	* @date: 2019年1月15日 下午1:53:13
	* @param hdfsUri 请保证以hdfs://ip:port的形式
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
		
	/**
	* @Title: existDir
	* @Description: 判断目录是否存在
	* @author: zhouwei
	* @date: 2019年1月16日 上午9:37:10
	* @param hdfsUri :hdfs://ip:port
	* @param path 绝对路径
	* @return
	* boolean
	*/
	public static  boolean existDir(String hdfsUri, String path){
        boolean flag = false;
        if(!isAbsolutePath(path)) {
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
	
	/**
	* @Title: existDir
	* @Description: 判断目录是否已经存在
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param fileSystem 文件系统实例
	* @param path 绝对路径
	* @return
	* boolean
	*/
	public static  boolean existDir(FileSystem fileSystem, String path){
        boolean flag = false;
        if(!isAbsolutePath(path)) {
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
	
	/**
	* @Title: mkdir
	* @Description: 创建目录，注意：执行的用户必须对目标路径具有写权限
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param hdfsUri
	* @param path
	* void
	*/
	public static void mkdir(String hdfsUri, String path) {
		if(!isAbsolutePath(path)) {
			return;
		}
        try {
            FileSystem fs = getFileSystem(hdfsUri);
            System.out.println("FilePath="+path);
            if(!existDir(fs, path)) {
            	// 创建目录
                fs.mkdirs(new Path(path));
            }else {
            	System.out.println(path+"已存在，无需重复创建");
            }
            //释放资源
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
	/**
	* @Title: mkdir
	* @Description: 创建目录，注意：执行的用户必须对目标路径具有写权限
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param fileSystem 文件系统实例
	* @param path
	* void
	*/
	public static void mkdir(FileSystem fileSystem, String path) {
		if(!isAbsolutePath(path)) {
			return;
		}
        try {
            System.out.println("FilePath="+path);
            if(!existDir(fileSystem, path)) {
            	// 创建目录
            	fileSystem.mkdirs(new Path(path));
            }else {
            	System.out.println(path+"已存在，无需重复创建");
            }  
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
	/**
	* @Title: putFileToHDFS
	* @Description: 将本地文件拷贝到HDFS，注意：执行的用户必须对destPath具有写权限
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param srcPath
	* @param hdfsUri
	* @param destPath
	* void
	*/
	public static void putFileToHDFS(String srcPath,String hdfsUri, String destPath) {
		if(!isAbsolutePath(destPath)) {
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
	
	/**
	* @Title: putFileToHDFS
	* @Description: 将本地文件拷贝到HDFS，注意：执行的用户必须对destPath具有写权限
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param srcPath
	* @param fileSystem
	* @param destPath
	* void
	*/
	public static void putFileToHDFS(String srcPath,FileSystem fileSystem, String destPath) {
		if(!isAbsolutePath(destPath)) {
			return;
		}
		try {
			FileInputStream fis=new FileInputStream(new File(srcPath));//读取本地文件
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
	
	/**
	* @Title: copyFileFromHDFS
	* @Description: hdfs文件拷贝到本地
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param hdfsUri
	* @param srcPath  hdfs的源文件路径
	* @param destPath 本地的目标路径
	* void
	*/
	public static void copyFileFromHDFS(String hdfsUri, String srcPath, String destPath) {
		if(!isAbsolutePath(srcPath)) {
			return;
		}
		try {
			//构建FileSystem
			FileSystem fs = getFileSystem(hdfsUri);
			//读取文件
			InputStream is= fs.open(new Path(srcPath));
			FileOutputStream os = new FileOutputStream(new File(destPath));
			IOUtils.copyBytes(is, os,2048, true);//保存到本地  最后 关闭输入输出流
			fs.close();
			System.out.println("拷贝 "+concat(hdfsUri, srcPath)+" 至 "+destPath);
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
	
	/**
	* @Title: copyFileFromHDFS
	* @Description: hdfs文件拷贝到本地
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param fileSystem
	* @param srcPath  hdfs的源文件路径
	* @param destPath 本地的目标路径
	* void
	*/
	public static void copyFileFromHDFS(FileSystem fileSystem, String srcPath, String destPath) {
		if(!isAbsolutePath(srcPath)) {
			return;
		}
		try {
			//构建FileSystem
			//读取文件
			InputStream is= fileSystem.open(new Path(srcPath));
			FileOutputStream os = new FileOutputStream(new File(destPath));
			IOUtils.copyBytes(is, os,2048, true);//保存到本地  最后 关闭输入输出流
			System.out.println("拷贝 "+concat(fileSystem.getUri().toString(),srcPath)+" 至 "+destPath);
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
	
	/**
	* @Title: rmDirOrFile
	* @Description: 删除目录或文件
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param hdfsUri
	* @param path
	* void
	*/
	public static void rmDirOrFile(String hdfsUri, String path) {
		if(!isAbsolutePath(path)) {
			return;
		}
        try {
            // 返回FileSystem对象
            FileSystem fs = getFileSystem(hdfsUri);
            // 删除文件或者文件目录  delete(Path f) 此方法已经弃用
            if( fs.delete(new Path(path),true)) {
            	System.out.println("删除 "+hdfsUri+path);
            }else {
            	System.out.println("文件删除失败！");
            }
            // 释放资源
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
	/**
	* @Title: rmDirOrFile
	* @Description: 删除目录或文件
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param fileSystem
	* @param path
	* void
	*/
	public static void rmDirOrFile(FileSystem fileSystem, String path) {
		if(!isAbsolutePath(path)) {
			return;
		}
        try {
            // 删除文件或者文件目录  delete(Path f) 此方法已经弃用
            if( fileSystem.delete(new Path(path),true)) {
            	System.out.println("删除 "+fileSystem.getUri()+path);
            }else {
            	System.out.println("文件删除失败！");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
	/**
	* @Title: readFile
	* @Description: 控制台查看hdfs文件内容
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param hdfsUri
	* @param filePath
	* void
	*/
	public static void readFile(String hdfsUri, String filePath) {
		if(!isAbsolutePath(filePath)) {
			return;
		}
        try {
			FileSystem fs = getFileSystem(hdfsUri);
			//读取文件
			Path path = new Path(filePath);
			if(!fs.exists(path)) {
				System.out.println(hdfsUri+filePath+"不存在，请检查");
				return;
			}
			if(fs.isDirectory(path)) {
				System.out.println(filePath+"是目录不是文件");
				return;
			}
			InputStream is=fs.open(path);
			//读取文件
			IOUtils.copyBytes(is, System.out, 2048, false); //复制到标准输出流
			is.close();
			fs.close();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
	/**
	* @Title: readFile
	* @Description: 控制台查看hdfs文件内容
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param fileSystem
	* @param filePath
	* void
	*/
	public static void readFile(FileSystem fileSystem, String filePath) {
		if(!isAbsolutePath(filePath)) {
			return;
		}
		
		try {
			Path path = new Path(filePath);
			if(!fileSystem.exists(path)) {
				System.out.println(fileSystem.getUri()+filePath+"不存在，请检查");
				return;
			}
			if(fileSystem.isDirectory(path)) {
				System.out.println(filePath+"是目录不是文件");
				return;
			}
			//读取文件
			InputStream is=fileSystem.open(path);
			//读取文件
			IOUtils.copyBytes(is, System.out, 2048, false); //复制到标准输出流
			is.close();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	/**
	* @Title: putFolderToHDFS
	* @Description: 将本地文件夹下的所有内容拷贝到hdfs指定目录下
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param srcPath
	* @param hdfsUri
	* @param destPath
	* @param isCover
	* void
	*/
	public static void putFolderToHDFS(String srcPath, String hdfsUri, String destPath, boolean isCover) {
		if(!isAbsolutePath(destPath)) {
			return;
		}
		File a = new File(srcPath);
		if(!a.exists()){
			System.out.println(srcPath+" 不存在");
			return;
		}
		if(!a.isDirectory()) {
			System.out.println(srcPath+" 不是文件夹");
			return;
		}
		String[] files = a.list();
		FileSystem fileSystem = getFileSystem(hdfsUri);       
        if(!existDir(fileSystem, destPath)) {
        	mkdir(fileSystem, destPath);
        }else{
        	//如果isCover为真，则覆盖原文件夹
        	if(isCover){
        		rmDirOrFile(fileSystem, destPath);
        		mkdir(fileSystem, destPath);
        		System.out.println("重新创建"+hdfsUri+destPath);
        	}else{
        		//如果不允许覆盖，提醒重名并退出
        		System.out.println(destPath+" 已存在，本次操作取消，如需覆盖原文件请设置参数isCover=true");
        		return;
        	}
        }
        try {								
			File temp = null;
			for (int i = 0; i < files.length; i++) {
			    if (srcPath.endsWith(File.separator)) {
			        temp = new File(srcPath + files[i]);
			    } else {
			        temp = new File(srcPath + File.separator + files[i]);
			    }
			    if (temp.isFile()) {
			        FileInputStream is = new FileInputStream(temp);
			        OutputStream os = fileSystem.create(new Path(destPath + "/" +temp.getName().toString()));
			        IOUtils.copyBytes(is, os, 2048, true);
			    }
			    if (temp.isDirectory()) {//如果是子文件夹
			    	putFolderToHDFS(srcPath + "/" + files[i], hdfsUri, destPath + "/" + files[i],isCover);
			    }
			}
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
	
	/**
	* @Title: putFolderToHDFS
	* @Description: 将本地文件夹下的所有内容拷贝到hdfs指定目录下，安全起见，默认不覆盖hdfs上的内容
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param srcPath
	* @param hdfsUri
	* @param destPath
	* void
	*/
	public static void putFolderToHDFS(String srcPath, String hdfsUri, String destPath){
		//为了安全起见，默认不开启覆盖原文件的模式
		putFolderToHDFS(srcPath, hdfsUri, destPath, false);
	}
	
	/**
	* @Title: putFolderToHDFS
	* @Description: 将本地文件夹下的所有内容拷贝到hdfs指定目录下
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param srcPath
	* @param fileSystem
	* @param destPath
	* @param isCover
	* void
	*/
	public static void putFolderToHDFS(String srcPath, FileSystem fileSystem, String destPath, boolean isCover) {
		if(!isAbsolutePath(destPath)) {
			return;
		}
		File a = new File(srcPath);
		if(!a.exists()){
			System.out.println(srcPath+" 不存在");
			return;
		}
		if(!a.isDirectory()) {
			System.out.println(srcPath+" 不是文件夹");
			return;
		}
		String[] files = a.list();     
        if(!existDir(fileSystem, destPath)) {
        	mkdir(fileSystem, destPath);
        }else{
        	//如果isCover为真，则覆盖原文件夹
        	if(isCover){
        		rmDirOrFile(fileSystem, destPath);
        		mkdir(fileSystem, destPath);
        		System.out.println("重新创建"+fileSystem.getUri()+destPath);
        	}else{
        		//如果不允许覆盖，提醒重名并退出
        		System.out.println(destPath+" 已存在，本次操作取消，如需覆盖原文件请设置参数isCover=true");
        		return;
        	}
        }
        try {								
			File temp = null;
			for (int i = 0; i < files.length; i++) {
			    if (srcPath.endsWith(File.separator)) {
			        temp = new File(srcPath + files[i]);
			    } else {
			        temp = new File(srcPath + File.separator + files[i]);
			    }
			    if (temp.isFile()) {
			        FileInputStream is = new FileInputStream(temp);
			        OutputStream os = fileSystem.create(new Path(destPath + "/" +temp.getName().toString()));
			        IOUtils.copyBytes(is, os, 2048, true);
			    }
			    if (temp.isDirectory()) {//如果是子文件夹
			    	putFolderToHDFS(srcPath + "/" + files[i], fileSystem, destPath + "/" + files[i],isCover);
			    }
			}
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
	
	/**
	* @Title: putFolderToHDFS
	* @Description: 将本地文件夹下的所有内容拷贝到hdfs指定目录下，安全起见，默认不覆盖hdfs上的内容
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param srcPath
	* @param fileSystem
	* @param destPath
	* void
	*/
	public static void putFolderToHDFS(String srcPath, FileSystem fileSystem, String destPath){
		//为了安全起见，默认不开启覆盖原文件的模式
		putFolderToHDFS(srcPath, fileSystem, destPath, false);
	}

	/**
	* @Title: copyFolderFromHDFS
	* @Description: 拷贝hdfs指定目录下的所有内容到本地
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param hdfsUri
	* @param srcPath
	* @param destPath
	* void
	*/
	public static void copyFolderFromHDFS(String hdfsUri, String srcPath, String destPath) {
		if(!isAbsolutePath(srcPath)) {
			return;
		}
		try {
			FileSystem fileSystem = getFileSystem(hdfsUri);
			Path path = new Path(srcPath);
			if(!fileSystem.exists(path)) {
				System.out.println(hdfsUri+srcPath+"不存在，请检查");
				return;
			}
			if(!fileSystem.isDirectory(path)) {
				System.out.println(srcPath+"不是目录，请选择正确的目录");
				return;
			}
			File file = new File(destPath);
			if(!file.exists()) {
				file.mkdirs();
				System.out.println("创建"+file.getPath()+"目录");
			}
			FileStatus[] fileStatusList = fileSystem.listStatus(path);
			for(FileStatus f : fileStatusList) {
				if(f.isDirectory()) {
					copyFolderFromHDFS(fileSystem, concat(srcPath,f.getPath().getName()), destPath+File.separator+f.getPath().getName());
				}else {
					copyFileFromHDFS(fileSystem, concat(srcPath,f.getPath().getName()), destPath+File.separator+f.getPath().getName());
				}
			}			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	/**
	* @Title: copyFolderFromHDFS
	* @Description: 拷贝hdfs指定目录下的所有内容到本地
	* @author: zhouwei
	* @date: 2019年1月17日 
	* @param fileSystem
	* @param srcPath
	* @param destPath
	* void
	*/
	public static void copyFolderFromHDFS(FileSystem fileSystem, String srcPath, String destPath) {
		if(!isAbsolutePath(srcPath)) {
			return;
		}
		try {
			Path path = new Path(srcPath);
			if(!fileSystem.exists(path)) {
				System.out.println(concat(fileSystem.getUri().toString(),srcPath)+"不存在，请检查");
				return;
			}
			if(!fileSystem.isDirectory(path)) {
				System.out.println(srcPath+"不是目录，请选择正确的目录");
				return;
			}
			File file = new File(destPath);
			if(!file.exists()) {
				file.mkdirs();
				System.out.println("创建"+file.getPath()+"目录");
			}
			FileStatus[] fileStatusList = fileSystem.listStatus(path);
			for(FileStatus f : fileStatusList) {
				if(f.isDirectory()) {
					copyFolderFromHDFS(fileSystem, concat(srcPath,f.getPath().getName()), destPath+File.separator+f.getPath().getName());
				}else {
					copyFileFromHDFS(fileSystem, concat(srcPath,f.getPath().getName()), destPath+File.separator+f.getPath().getName());
				}
			}			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}//class
