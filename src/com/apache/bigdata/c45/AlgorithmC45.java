package com.apache.bigdata.c45;


import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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



public class AlgorithmC45 extends Configured implements Tool {
	
    public static Data currentsplit=new Data();
    public static List <Data> splitted=new ArrayList<Data>();;
    public static int current_index=0;
    public static String output_path;
    public static List <String> ruleList=new ArrayList<String>();
    public static List <String> stepList=new ArrayList<String>();
    
    
    public static void main(String[] args) throws Exception {
	 
      			  splitted.add(currentsplit);
    			  int res=0;
				  int split_index=0;
				  double gainratio=0;
				  double best_gainratio=0;
				  double entropy=0;
				  String classLabel=null;
				  int total_attributes=8;
				  int split_size=splitted.size();
				  GainRatio gainObj;
				  Data newnode;
				  output_path=args[1];
				  int step=0;
				  int numberLevel = 0;
	 
	              while(split_size>current_index){ 	            	  
		              deleteOutPutDir(args[1]);
		              // deleteFile();
		              System.out.println("Split size" +split_size + "current_index"+current_index);
		              currentsplit=(Data) splitted.get(current_index); 
		              
		              gainObj=new GainRatio();
		              res = ToolRunner.run(new Configuration(), new AlgorithmC45(), args);
					  int j=0;
					  int temp_size;
					  gainObj.getcount();
					  entropy=gainObj.currNodeEntophy();
					  classLabel=gainObj.majorityLabel();
					  currentsplit.setClassLabel(classLabel);
					  
					  if(entropy!=0 && currentsplit.att_index.size()!=total_attributes){					    
					    	
					    	System.out.println("Entropy NOT zero SPLIT INDEX::    "+entropy);
					    	best_gainratio=0;
					    //	System.out.println("The att value":currentsplit.att_value.toString());
					    	for(j=0;j<total_attributes;j++){		//Finding the gain of each attribute
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
						    //int tred=-1;
						    System.out.println("INDEX ::  "+split_index);
						    System.out.println("SPLITTING VALUES  "+attr_values_split);
						    
						    //To write the steps to the new file:
						    ++step;
						    stepList.add("Step :"+step+"	Attribute name :"+nodeName(split_index)+"	Splitting values :"+attr_values_split);						    	    
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
				    	        numberLevel=0; 
				    			for(int val=0;val<temp_size;val++)  
				    			{
	    		    	         rule=rule+" "+nodeName(currentsplit.att_index.get(val))+" "+currentsplit.att_value.get(val);
	    		    	         numberLevel++;
	    	                     }
				    			
				    	rule=rule+" "+currentsplit.getClassLabel();
				    	ruleList.add(rule);
				    	if(entropy!=0.0)
				    			System.out.println("Enter rule in file:: "+rule);
				    	else
				    			System.out.println("Enter rule in file Entropy zero ::   "+rule);
					  	}
					  
					    split_size=splitted.size();
					    System.out.println("TOTAL NODES::    "+split_size);
					    current_index++;
                    }
	              stepList.add("TOTAL NUMBER OF LEVELS :"+numberLevel);
	              writeToFile("rule");
	              writeToFile("steps");
	              System.out.println("COMPLEEEEEEEETEEEEEEEEEE");
	              System.out.println("TOTAL NUMBER OF LEVELS :"+numberLevel);
	            
	    	    System.exit(res);
  }
    
    
    	public static void writeToFile(String fileName) {
		    try  {
		    	int i = 0;
		    	if (fileName.equals("rule"))
		    	{
				while (i < ruleList.size()) {
					   BufferedWriter bw = new BufferedWriter(new FileWriter(new File(output_path+"/"+fileName+".txt"),true));    
					   bw.write(ruleList.get(i));
					   bw.newLine();
					   bw.close();
					   i++;
						}
		    	}
		    	else if (fileName.equals("steps"))
		    	{
		    		while (i < stepList.size()) {
						   BufferedWriter bw = new BufferedWriter(new FileWriter(new File(output_path+"/"+fileName+".txt"),true));    
						   bw.write(stepList.get(i));
						   bw.newLine();
						   bw.close();
						   i++;
							}
		    	}
			    	
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
		return 0;
 	}
  
  
 	
 	public static void deleteOutPutDir(String path) throws IOException
 	{  
	      File file = new File(path);
		  if(file.exists())
		  {
			  FileUtils.deleteDirectory(file);
			  System.out.println("File deleted");
			  
		  }
 	}
 	
 	
	  public static String nodeName(int i)
	  {
		  String nodename;
		  switch (i) {
	      case 0:  nodename = "CLASS";
	               break;
	      case 1:  nodename = "AGE";
	               break;
	      case 2:  nodename = "MENOPAUSE";
	               break;
	      case 3:  nodename = "TUMOR-SIZE";
	               break;
	      case 4:  nodename = "INV-NODES";
	               break;
	      case 5:  nodename = "NODE-CAPS";
	               break;
	      case 6:  nodename = "DEG-MALIG";
	               break;
	      case 7:  nodename = "BREAST";
	      		   break;
	      case 9:  nodename = "BREAST-QUAD";
	               break;
		 default: nodename = "NO_NAME";
	               break;
	  }
		  return nodename;
	  }
	  
	  }

     