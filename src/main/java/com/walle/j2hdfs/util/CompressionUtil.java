/**
 * 
 */
package com.walle.j2hdfs.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.Lists;

/**
* @ClassName: CompressionUtil
* @Description: 编码压缩工具类
* @author: zhouwei
* @date: 2019年1月28日
*/
public class CompressionUtil {
	
	/**
	* @Title: tarCompression
	* @Description: 将本地文件/文件夹打包为tar包
	* @author: zhouwei
	* @date: 2019年1月30日 
	* @param srcPath
	* @param destPath
	* @return
	* boolean
	*/
	public static boolean tarCompression(String srcPath, String destPath) {
		try {
			
			List<File> fileList = getFileList(srcPath);
			if(fileList == null) return false;
			//如果destPath不是以.tar结尾，则自动加上后缀.tar
			if(!destPath.endsWith(".tar")) {
				destPath += ".tar";
			}
			File destFile = new File(destPath);
			FileOutputStream fos = new FileOutputStream(destFile);
			TarArchiveOutputStream taros = new TarArchiveOutputStream(fos);
			FileInputStream fis = null;
			BufferedInputStream bis = null;
			int len = 0;
			byte[] data = new byte[2048];
			String parentPath = fileList.get(0).getParent();
			for(File f : fileList) {
				TarArchiveEntry tae = new TarArchiveEntry(f);
				tae.setName(getRelativePath(parentPath, f.getAbsolutePath()));
				taros.putArchiveEntry(tae);
				if(f.isDirectory()) {
					taros.closeArchiveEntry();
					continue;
				}
				fis = new FileInputStream(f);
				bis = new BufferedInputStream(fis);
				while((len = bis.read(data)) != -1) {
					taros.write(data,0,len);
				}
				taros.closeArchiveEntry();
				if(bis != null) bis.close();
				if(fis != null) fis.close();
			}		
			if(taros != null) taros.close();
			if(fos !=null) fos.close();		
			return true;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	* @Title: getRelativePath
	* @Description: 获取subDirectory相对于directory的相对路径
	* @author: zhouwei
	* @date: 2019年1月30日 
	* @param directory
	* @param subDirectory
	* @return
	* String
	*/
	public static String getRelativePath(String directory, String subDirectory) {
		String relativePath = "";
		String os = System.getProperty("os.name");
		if(os.contains("Windows")) {
			String[] ss1 = directory.split("\\\\");
			String[] ss2 = subDirectory.split("\\\\");
			for(int i = ss1.length; i<ss2.length; i++) {
				if(i<ss2.length-1) {
					relativePath += ss2[i]+"\\";
				}else {
					relativePath += ss2[i];
				}
			}
		}else {		
			String[] ss1 = directory.split("/");
			String[] ss2 = subDirectory.split("/");
			for(int i = ss1.length; i<ss2.length; i++) {
				if(i<ss2.length-1) {
					relativePath += ss2[i]+"/";
				}else {
					relativePath += ss2[i];
				}
			}
		}
		return relativePath;
	}
	/**
	* @Title: tarDecompression
	* @Description: 将tar包解压为文件/文件夹
	* @author: zhouwei
	* @date: 2019年1月30日 
	* @param srcPath
	* @param destPath
	* @return
	* boolean
	*/
	public static boolean tarDecompression(String srcPath, String destPath) {
		TarArchiveInputStream taris = null;
		FileInputStream fis = null;
		try {
			File file = new File(srcPath);
			fis = new FileInputStream(file);
			taris = new TarArchiveInputStream(fis);
			TarArchiveEntry tae = null;
			File destDir = new File(destPath);
			if(!destDir.exists()){
				destDir.mkdirs();
			}
			while((tae = taris.getNextTarEntry()) != null) {
				BufferedOutputStream bos = null;
				FileOutputStream fos = null;
				String dirPath = destPath+File.separator+tae.getName();				
				if(tae.isDirectory()) {	
					new File(dirPath).mkdirs();
					continue;
				}
				File tmp = new File(dirPath);
				fos = new FileOutputStream(tmp);
				bos = new BufferedOutputStream(fos);
				int len = 0;
				byte[] buf = new byte[1024];
				while((len = taris.read(buf, 0, 1024)) != -1) {
					bos.write(buf, 0, len);
				}
				if(bos != null) bos.close();
				if(fos != null) fos.close();
			}
			if(taris != null) taris.close();
			if(fis != null) fis.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return true;
	}
	
	/**
	* @Title: getFileList
	* @Description: 获取srcPath下子文件/子文件夹的目录列表
	* @author: zhouwei
	* @date: 2019年1月30日 
	* @param srcPath
	* @return
	* List<File>
	*/
	public static List<File> getFileList(String srcPath) {
		File file = new File(srcPath);
		List<File> fileList = Lists.newArrayList();
		if(!file.exists()) {
			System.out.println(srcPath+"不存在");
			return null;
		}
		fileList.add(file);
		File[] files = file.listFiles();
		if(files == null) return fileList;
		for(File f : files) {
			if(f.isFile()) {
				fileList.add(f);
			}else {
				getFileList(f, fileList);
			}					
		}
		return fileList;
	}
	private static List<File> getFileList(File srcFile,List<File> fileList) {
		fileList.add(srcFile);
		File[] files = srcFile.listFiles();
		if(files == null) return fileList;
		for(File f : files) {
			if(f.isFile()) {
				fileList.add(f);
			}else {
				getFileList(f, fileList);
			}							
		}
		return fileList;
	}
}
