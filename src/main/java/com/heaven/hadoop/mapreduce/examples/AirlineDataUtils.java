package com.heaven.hadoop.mapreduce.examples;

import org.apache.hadoop.io.Text;

public class AirlineDataUtils {
	public static String mergeStringArray(String[] arrays,String delimiter){
		StringBuilder sb=new StringBuilder();
		for(String value:arrays){
			sb.append(value).append(delimiter);
		}
		return sb.toString();
	}
	public static String[] getSelectResultPerRow(Text text){
		String [] contents=text.toString().split(",");
		String [] outputArray=new String[6];
		outputArray[0 ]=getDate(contents);
		outputArray[1 ]=getDepartureTime(contents);
		outputArray[2 ]=getArrivalTime(contents);
		outputArray[3 ]=getOrigin(contents);
		outputArray[4 ]=getDestination(contents);
		outputArray[5 ]=getDistance(contents);
		return outputArray;
	}
	public static String getDate(String[] contents) {
		StringBuilder builder=new StringBuilder();
		builder.append(contents[0]).append("-").append(contents[1]).append("-").append(contents[2]);
		return null;
	}
	public static String getDepartureTime(String[] contents) {
		return contents[4];
	}
	public static String getArrivalTime(String[] contents) {
		return contents[6];
	}
	public static String getOrigin(String[] contents) {
		return contents[16];
	}
	public static String getDestination(String[] contents) {
		return contents[17];
	}
 	public static String getDistance(String[] contents) {
		return contents[18];
	}

}
