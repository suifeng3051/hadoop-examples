package com.heaven.hadoop.io;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PutMerge {
	public static void main(String[] args) throws IOException {
		Configuration config=new Configuration();
		FileSystem hdfs=FileSystem.get(config);
		FileSystem local=FileSystem.getLocal(config);
		Path inputDir=new Path("d:/input");
		Path hsfsFile=new Path("/user/result/mergedinput.txt");
		
		FileStatus[] inputFiles=local.listStatus(inputDir);
		FSDataOutputStream out=hdfs.create(hsfsFile);
		for(int i=0;i<inputFiles.length;i++){
			FSDataInputStream in=local.open(inputFiles[i].getPath());
			byte[] buffer=new byte[1024];
			int byteRead=0;
			while((byteRead=in.read(buffer))>0){
				out.write(buffer, 0,byteRead );
			}
			in.close();
		}
		out.close();
	}
}
