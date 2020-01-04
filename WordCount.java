import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Scanner;
import java.net.URI;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	private Path[] files;
	private ArrayList<String> stopwords = new ArrayList<String>();
	
	
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {	
		StringTokenizer itr = new StringTokenizer(value.toString());		
		
		if(stopwords.size() == 0)
		{			
			files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.getLocal(conf);
			FSDataInputStream in = fs.open(files[0]);
			InputStreamReader inputStreamReader = new InputStreamReader(in);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
	
			String toRead = bufferedReader.readLine();
			while(toRead != null)
			{
				stopwords.add(toRead);
				toRead = bufferedReader.readLine();
			}
		
			/*check
			System.out.println(stopwords.get(0));
			System.out.println(stopwords.get(1));
			System.out.println(stopwords.size());
			*/
		}
		
		while (itr.hasMoreTokens()) {
			String toAdd = itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
			if (toAdd.isEmpty())
			{
				continue;
			}
			if (stopwords.contains(toAdd))
			{
				continue;
			}
			word.set(Character.toString(toAdd.charAt(0)));
			context.write(word, one);
		}
	}
	}
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    DistributedCache.addCacheFile(new URI("wocoin/stopwords.txt"), conf);
    Job job = Job.getInstance(conf, "word count");
    job.setJar("wc.jar");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


}	
