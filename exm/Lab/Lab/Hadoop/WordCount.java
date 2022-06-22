package MyPack;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


	/**
	 *
	 * @author admin
	 */
	public class WordCount {
	    public static class MyWCMap extends Mapper<LongWritable, Text, Text, IntWritable>
	    {
	        @Override
	        public void map(LongWritable ipkey, Text value, Context context) throws IOException, InterruptedException
	        {
	        	Text opkey=new Text();
	          String line = value.toString();
	          StringTokenizer tokenizer = new StringTokenizer(line);
	          while(tokenizer.hasMoreTokens())
	          {
	              opkey.set(tokenizer.nextToken());
	              context.write(opkey, new IntWritable(1));
	             
	          }
	          }
	        }
	    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>
	     {
	         @Override
	         public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException
	         {
	             int sum=0;
	             for(IntWritable x:values)
	             {
	                 sum +=x.get();
	             }
	             context.write(key, new IntWritable(sum));
	         }
	     }
	    public static void main(String args[]) throws Exception
	    {
	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf,"WordCount");
	        job.setJarByClass(WordCount.class);
	        job.setMapperClass(MyWCMap.class);
	        job.setReducerClass(Reduce.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        job.setNumReduceTasks(1);
	        
	        Path InputPath = new Path(args[0]);
	        Path OutputPath = new Path(args[1]);
	    
	        FileInputFormat.addInputPath(job, InputPath);
	        FileOutputFormat.setOutputPath(job, OutputPath);
	        
	        OutputPath.getFileSystem(conf).delete(OutputPath, true);
	        
	        System.exit(job.waitForCompletion(true)?0:1);
	    }
	     }

	

	


