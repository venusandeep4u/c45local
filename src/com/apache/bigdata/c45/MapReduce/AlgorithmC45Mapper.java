package com.apache.bigdata.c45.MapReduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.apache.bigdata.*;
import com.apache.bigdata.c45.AlgorithmC45;
import com.apache.bigdata.c45.datamodel.Data;

public class AlgorithmC45Mapper extends Mapper<Object, Text, Text, IntWritable> {

private final static IntWritable one = new IntWritable(1);
private Text attValue = new Text();
private Text cLabel = new Text();
private int i;
private String token;
public static int no_Attr;
//public static int splitAttr[];
private int flag=0;


public void map(Object key, Text value, Context context) throws InterruptedException,IOException {

  AlgorithmC45 id=new AlgorithmC45();
  Data split=null;
  int size_split=0;
  split=id.currentsplit;
  
  String line = value.toString();      //changing input instance value to string
  StringTokenizer itr = new StringTokenizer(line,",");
  int index=0;
  String attr_value=null;
  no_Attr=itr.countTokens()-1;
  String attr[]=new String[no_Attr];
  boolean match=true;
  for(i =0;i<no_Attr;i++)
  {
	  attr[i]=itr.nextToken();		//Finding the values of different attributes
  }
  String classLabel=itr.nextToken();
  size_split=split.att_index.size();
  for(int count=0;count<size_split;count++)
  {
	  index=(Integer) split.att_index.get(count);
	  attr_value=(String)split.att_value.get(count);
	 if(attr[index].equals(attr_value))   //may also use attr[index][z][1].contentEquals(attr_value)
	 {
		 //System.out.println("EQUALS IN MAP  nodes  "+attr[index]+"   inline  "+attr_value);
	 }
	 else
	 {
		// System.out.println("NOT EQUAL IN MAP  nodes  "+attr[index]+"   inline  "+attr_value);
		 match=false;
		 break;
	 }
	  
  }
  
  
  //id.attr_count=new int[no_Attr];

  if(match)
  {
	  for(int l=0;l<no_Attr;l++)
	  {  
		  if(split.att_index.contains(l))
		  {
			  
		  }
		  else
		  {
			  token=l+" "+attr[l]+" "+classLabel;
			  attValue.set(token);
			  
			  context.write(attValue, one);
		  }
	 
  	}
	  if(size_split==no_Attr)
	  {
		  token=no_Attr+" "+"null"+" "+classLabel;
		  attValue.set(token);
		  context.write(attValue, one);
	  	}
   }
 }
 
}

  
