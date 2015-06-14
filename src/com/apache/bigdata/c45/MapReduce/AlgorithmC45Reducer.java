package com.apache.bigdata.c45.MapReduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.apache.bigdata.c45.AlgorithmC45;

public  class AlgorithmC45Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	

public void reduce(Text key,Iterable<IntWritable> values ,Context context) throws IOException,InterruptedException
 {

  int sum = 0;
  String line = key.toString();
  StringTokenizer itr = new StringTokenizer(line);

	for(IntWritable val:values)
	{
		sum=sum+val.get();
		
	}
    context.write(key, new IntWritable(sum));
	 // writeToFile(key+" "+sum);
	  int index=Integer.parseInt(itr.nextToken());
	  String value=itr.nextToken();
	  String classLabel=itr.nextToken();
	  int count=sum;
	}
	
 

/*public static void writeToFile(String text) {
    try {
    	
    	AlgorithmC45 id=new AlgorithmC45();
    	BufferedWriter bw = new BufferedWriter(new FileWriter(new File("/home/mkv/workspace/AlgorithmC45/input/intermidiate.txt"), true));    
    	bw.write(text);
            bw.newLine();
            bw.close();
    } catch (Exception e) {
    }
}
*/

}
 

