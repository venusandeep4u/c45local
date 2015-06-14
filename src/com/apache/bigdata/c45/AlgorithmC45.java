package com.apache.bigdata.c45;


import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.apache.bigdata.c45.MapReduce.AlgorithmC45Mapper;
import com.apache.bigdata.c45.MapReduce.AlgorithmC45Reducer;
import com.apache.bigdata.c45.datamodel.Data;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.*;



public class AlgorithmC45 extends Configured implements Tool {


    public static Data currentsplit=new Data();
   
    public static List <Data> splitted=new ArrayList<Data>();;
    
    public static int current_index=0;

  public static void main(String[] args) throws Exception {
	 
	  AlgorithmC45Mapper map=new AlgorithmC45Mapper();
	  splitted.add(currentsplit);
	  
	  int res=0;
	  double bestGain=0;
	  boolean stop=true;
	  boolean outerStop=true;
	  int split_index=0;
	  double gainratio=0;
	  double best_gainratio=0;
	  double entropy=0;
	  String classLabel=null;
	  int total_attributes=map.no_Attr;
	  total_attributes=4;
	  int split_size=splitted.size();
	  GainRatio gainObj;
	  Data newnode;
	 
	  while(current_index==0)
        { 
		  deleteIntermidiateFile();
		  deleteOutPutDir();
		 // deleteFile();
		  System.out.println("Split size" +split_size + "current_index"+current_index);
		 
    	  currentsplit=(Data) splitted.get(current_index); 
    	  gainObj=new GainRatio();
    	
	    res = ToolRunner.run(new Configuration(), new AlgorithmC45(), args);
	  
	   // System.out.println("Current  NODE INDEX . ::"+current_index);
    	
    	
	    int j=0;
	    int temp_size;
	    gainObj.getcount();
	    entropy=gainObj.currNodeEntophy();
	    classLabel=gainObj.majorityLabel();
	    currentsplit.setClassLabel(classLabel);
	    
	    
	    
    	   if(entropy!=0 && currentsplit.att_index.size()!=total_attributes)
	    {
	    	System.out.println("");
	    	System.out.println("Entropy NOT zero SPLIT INDEX::    "+entropy);
	    	best_gainratio=0;
	 
	    for(j=0;j<total_attributes;j++)		//Finding the gain of each attribute
	    {
	    	
	    	  if(currentsplit.att_index.contains(j))  // Splitting all ready done with this attribute
	  	      {
	  		  // System.out.println("Splitting all ready done with  index  "+j);
	  	      }
	  	      else
	  	      {
	  	    	gainratio=gainObj.gainratio(j,entropy);
	  	    	
	  		  if(gainratio>=best_gainratio)
	  		  {
	  			split_index=j;
	 
	  			best_gainratio=gainratio;
	  		   }
	  	      
	    	}
	    }
	    
	    
	    
	    
	    String attr_values_split=gainObj.getvalues(split_index);
	    StringTokenizer attrs = new StringTokenizer(attr_values_split);
	    int number_splits=attrs.countTokens(); //number of splits possible with  attribute selected
	    String red="";
	    int tred=-1;
	    
	    
	    
	    System.out.println(" INDEX ::  "+split_index);
	    System.out.println(" SPLITTING VALUES  "+attr_values_split);
	    
	    for(int splitnumber=1;splitnumber<=number_splits;splitnumber++)
	    {
	  
	    	
	
	    	temp_size=currentsplit.att_index.size();
	    	newnode=new Data(); 
	    	for(int y=0;y<temp_size;y++)   // CLONING OBJECT CURRENT NODE
	    	{
	    	
	    		newnode.att_index.add(currentsplit.att_index.get(y));
	    		newnode.att_value.add(currentsplit.att_value.get(y));
	    	}
	    	red=attrs.nextToken();
	    	
	    	newnode.att_index.add(split_index);
	    	newnode.att_value.add(red);
	    	splitted.add(newnode);
	    }
	    }
	    else
	    {
	    	System.out.println("");
	    	String rule="";
	    	temp_size=currentsplit.att_index.size();
	    	for(int val=0;val<temp_size;val++)  
	    	{
	    	
	    	//rule=rule+" "+currentsplit.att_index.get(val)+" "+currentsplit.att_value.get(val);
	    	rule=rule+" "+nodeName(currentsplit.att_index.get(val))+" "+currentsplit.att_value.get(val);
	    	}
	    	rule=rule+" "+currentsplit.getClassLabel();
	    	writeRuleToFile(rule);
	    	if(entropy!=0.0)
	    		System.out.println("Enter rule in file:: "+rule);
	    	else
	    	System.out.println("Enter rule in file Entropy zero ::   "+rule);
	    	
	    	
	    }
	    
	    split_size=splitted.size();
	    System.out.println("TOTAL NODES::    "+split_size);
	    
	 
	    
	    current_index++;
	  
	   
	    
        }
	  
	  System.out.println("COMPLEEEEEEEETEEEEEEEEEE");
	    	System.exit(res);

	  
  }
  public static void writeRuleToFile(String text) {
	    try {

	            
	    	BufferedWriter bw = new BufferedWriter(new FileWriter(new File("hdfs://localhost:9000/home/mkv/workspace/AlgorithmC45/input/rule.txt"), true));    
	    	bw.write(text);
	            bw.newLine();
	            bw.close();
	    } catch (Exception e) {
	    }
	}
  
  
  
  public int run(String[] args) throws Exception {
    
   
    Configuration conf=getConf();
	Job job=new Job(conf,"Algorithem C45 Implementation");
	job.setJarByClass(AlgorithmC45.class);
    job.setMapperClass(AlgorithmC45Mapper.class);
	job.setReducerClass(AlgorithmC45Reducer.class);
	FileInputFormat.addInputPath(job,  new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.waitForCompletion(true) ;
	
	DistributedCache.addCacheFile(new URI("dfs://localhost:9000"
		      + "/user/mkv/playtennis.txt"),
		      job.getConfiguration());
	return 0;
   
  }
  
  public static void deleteIntermidiateFile()
  {   
	  
	 /* String fileName ="hdfs://localhost:9000/home/mkv/workspace/AlgorithmC45/input/intermidiate.txt";
	  System.out.println(fileName);
	  File file = new File(fileName);
	  if(file.exists())
	  {
		  file.delete();
		  System.out.println("File deleted");
		  
	  }
	  */
		  
  }
  
  public static void deleteOutPutDir() throws IOException
  {   
	  FileSystem fs = FileSystem.get(new Configuration());
	  fs.delete(new Path("hdfs://localhost:9000/output"), true);
	  /*
	  String fileName ="hdfs://localhost:9000/home/mkv/workspace/AlgorithmC45/output";
	  File file = new File(fileName);
	  if(file.exists())
	  {
		  FileUtils.deleteDirectory(file);
		  System.out.println("File deleted");
		  
	  }
	  */
		  
  }
  public static String nodeName(int i)
  {
	  String nodename;
  
	  switch (i) {
      case 1:  nodename = "Class";
               break;
      case 2:  nodename = "Age";
               break;
      case 3:  nodename = "Menopause";
               break;
      case 4:  nodename = "tumor-size";
               break;
      case 5:  nodename = "inv-nodes";
               break;
      case 6:  nodename = "node caps";
               break;
      case 7:  nodename = "deg-malig";
               break;
      case 8:  nodename = "breast";
      case 9:  nodename = "breast-quad";
      default: nodename = "NO_NAME";
      break;
  }
	  return nodename;
  }
  
}