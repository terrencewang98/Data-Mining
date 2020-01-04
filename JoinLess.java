import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class JoinLess {

  public static class RMapper
       extends Mapper<Object, Text, Text, Text>{
	
  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                    	
		StringTokenizer itr = new StringTokenizer(value.toString());		
		
		String a = itr.nextToken();
		String b = itr.nextToken();
		
		String toText = "R," + a + "," + b;
		Text toValue = new Text(toText);
		Text toKey = new Text(b);
		context.write(toKey, toValue);
	}
  }
  
  public static class SMapper
       extends Mapper<Object, Text, Text, Text>{
	
  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                    	
		StringTokenizer itr = new StringTokenizer(value.toString());		
		
		String a = itr.nextToken();
		String b = itr.nextToken();
		
		int count = Integer.parseInt(a);
		String toText = "S," + a + "," + b;
		Text toValue = new Text(toText);
		
		for(int i = 0; i < count; i++)
		{
			String stringKey = Integer.toString(i);
			Text toKey = new Text(stringKey);
			context.write(toKey, toValue);
		}
	}
  }

  public static class JoinLessReducer
       extends Reducer<Text,Text,Text,Text> {
   
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
	ArrayList<String> RList = new ArrayList<String>();
	ArrayList<String> SList = new ArrayList<String>(); 
    
    for(Text v: values)
    {
		String value = v.toString();
		System.out.println("value: " + value);
		String[] fromValues = value.split(",");	
		for(int i = 0; i < 3; i++)
		{
			System.out.println(fromValues[i]);
		}
		if(fromValues[0].equals("R"))
		{
			System.out.println("fromValues1: " + fromValues[1]);
			System.out.println("fromValues2: " + fromValues[2]);
			
			String toAdd = fromValues[1] + " " + fromValues[2]; 
			RList.add(toAdd);
		}
		if(fromValues[0].equals("S"))
		{
			System.out.println("fromValues1: " + fromValues[1]);
			System.out.println("fromValues2: " + fromValues[2]);
			
			String toAdd = fromValues[1] + " " + fromValues[2]; 
			SList.add(toAdd); 
		}
	}
    
    for(int i = 0; i < RList.size(); i++)
    { 
		for(int j = 0; j < SList.size(); j++)
		{
			Text toKey = new Text(" ");
			System.out.println("RList: " + RList.get(i));
			System.out.println("SList: " + SList.get(j)); 
			Text toValue = new Text(RList.get(i) + " " + SList.get(j));
			context.write(toKey, toValue);
		}
    }
  }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "join less");
    job.setJar("j1.jar");
    job.setJarByClass(JoinLess.class);
	MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RMapper.class);
	MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SMapper.class);
    job.setReducerClass(JoinLessReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


}	
