package com.heaven.hadoop.hdfs.tools;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeHadoopFiles {
	
	private final static Logger log = LoggerFactory.getLogger(MergeHadoopFiles.class);
	public static void main(String[] argv) throws IOException, URISyntaxException {
		
		long maxsize;
		int index=0;
		long cursize=0;
		int sindex,eindex;
		SimpleDateFormat sdf = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss.SSS");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.164:9000/flumetest/2.txt"),conf);
		if(argv.length!=4 && argv.length!=5){
			log.error("paramater error\n");
			return;
		}
	    if(argv.length==4){
	    	maxsize=Long.MAX_VALUE;
	    	sindex=Integer.parseInt(argv[2]);
	    	eindex=Integer.parseInt(argv[3]);
	    }
	    else{
	    	maxsize=Long.parseLong(argv[2])*1024;
	    	sindex=Integer.parseInt(argv[3]);
	    	eindex=Integer.parseInt(argv[4]);
	    }
	    
	    
		Path inputDir = new Path(argv[0]);
		FileStatus[] inputFiles = fs.listStatus(inputDir);
		String str=inputFiles[0].getPath().getName().substring(sindex, eindex);
		Path hdfsFile = new Path(argv[1]+String.valueOf(index));
		log.info("\n"+hdfsFile.toString());
		FSDataOutputStream out = fs.create(hdfsFile);
		for (int i = 0; i < inputFiles.length; i++) {
			if(!compare(str,inputFiles[i],sindex,eindex)){
	
					out.close();
					index=0;
					str=inputFiles[i].getPath().getName().toString().substring(sindex, eindex);
					hdfsFile = new Path(argv[1]+str+"-"+String.valueOf(index));
					out=fs.create(hdfsFile);
					cursize=0;
					log.info("\n"+hdfsFile.toString());
			}
			else{
					if(cursize+inputFiles[i].getLen() > maxsize  ){
					out.close();
					index++;
					hdfsFile = new Path(argv[1]+str+"-"+String.valueOf(index));
					out=fs.create(hdfsFile);
					cursize=0;
					log.info("\n"+hdfsFile.toString());
				}
			}
			cursize+=inputFiles[i].getLen();
			FSDataInputStream in = fs.open(inputFiles[i].getPath());
			byte buffer[] = new byte[2048];
			int bytesRead = 0;
			while ((bytesRead = in.read(buffer)) > 0) {
				out.write(buffer,0, bytesRead);
			}
			log.info("\t"+i+"th file: "+ inputFiles[i].getPath().toString() + " merged completed at " + sdf.format(new Date()));
			in.close();
		}
		out.close();
	}
	
	public static boolean compare(String s ,FileStatus st,int sindex,int eindex){
		
		return s.equals(st.getPath().getName().toString().substring(sindex, eindex));	
	}

}
