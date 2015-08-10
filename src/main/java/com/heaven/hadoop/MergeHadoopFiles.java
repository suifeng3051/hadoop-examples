package com.heaven.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MergeHadoopFiles {

	public static void main(String[] argv1) throws IOException, URISyntaxException {
		String[] argv={"/user/input","/user/output","CDR_FIX","1.5"};
		String srcDir=argv[0];
		String dstPath=argv[1];
		String filePrefix=argv[2];
		float maxMergeSize=Float.valueOf(argv[3])*1024*1024;
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.29.131:9000"),conf);
		int index = 0;
		long cursize = 0;
		Path srcDirPath = new Path(srcDir);
		FileStatus[] inputFiles = fs.listStatus(srcDirPath);
		String newFilePath=dstPath+"/"+ filePrefix+"_"+String.valueOf(index);
		Path hdfsFile = new Path(newFilePath);
		FSDataOutputStream out = fs.create(hdfsFile);
		for (int i = 0; i < inputFiles.length; i++) {
			if (cursize + inputFiles[i].getLen() > maxMergeSize) {
				out.close();
				index++;
				newFilePath=dstPath+"/"+ filePrefix+"_"+String.valueOf(index);
				hdfsFile = new Path(newFilePath);
				out = fs.create(hdfsFile);
				cursize = 0;
			}
			cursize += inputFiles[i].getLen();
			FSDataInputStream in = fs.open(inputFiles[i].getPath());

			IOUtils.copy(in, out);
			in.close();
		}
		out.close();
	}
}
