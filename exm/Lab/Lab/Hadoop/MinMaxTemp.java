package MyPack;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
*
* @author admin
*/
public class MinMaxTemp {
public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//Mapper
/**
* @method map
* <p>This method takes the input as text data type and and tokenizes input
* by taking whitespace as delimiter. The first token goes year and second token is
temperature,
* this is repeated till last token. Now key value pair is made and passed to reducer.
* @method_arguments key, value, output, reporter
* @return void
*/
//Defining a local variable k of type Text
Text CityMin = new Text();
Text CityMax = new Text();
/*
* (non-Javadoc)
* @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object,
org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
*/
/**
*
* @param key
* @param value
* @param context
* @throws IOException
* @throws InterruptedException
*/
@Override
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//Converting the record (single line) to String and storing it in a String variable line
String line = value.toString();
//StringTokenizer is breaking the record (line) according to the delimiter whitespace
StringTokenizer tokenizer = new StringTokenizer(line,",");
//Iterating through all the tokens and forming the key value pair
int i = 1;
String CName="",CMin="",CMax="";
int MaxTemp=-200,MinTemp=200;
while (tokenizer.hasMoreTokens()) {
//The first token is going in year variable of type string
switch (i) {
case 1:
CName = tokenizer.nextToken();
System.out.println("City Name:" +CName);
CMin = CName+"_Min";
CMax=CName+"_Max";
//System.out.println("City Name:", CMin);
break;
case 2:
tokenizer.nextToken();
break;
case 3:
//Takes next token and removes all the whitespaces around it and then convert it into an integer value
MaxTemp=Integer.parseInt(tokenizer.nextToken());
break;
case 4:
MinTemp=Integer.parseInt(tokenizer.nextToken());
break;
}
i++;
}
CityMax.set(CMax);
context.write(CityMax, new IntWritable(MaxTemp));
CityMin.set(CMin);
context.write(CityMin, new IntWritable(MinTemp));
}
}
//Reducer
/**
* @interface Reducer
* <p>Reduce class is static and extends MapReduceBase and implements Reducer
* interface having four hadoop generics type Text, IntWritable, Text, IntWritable.
*/
public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
/**
* @param key
* @param values
* @param context
* @throws java.io.IOException
* @throws java.lang.InterruptedException
* @method reduce
* <p>This method takes the input as key and list of values pair from mapper, it does aggregation * based on keys and produces the final output.
* @method_arguments key, values, output, reporter
* @return void
*/
/*
* (non-Javadoc)
* @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator,
org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
*/
@Override
public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
/*
* Iterates through all the values available with a key and if the integer variable temperature
* is greater than maxtemp, then it becomes the maxtemp
*/
//Defining local variables MinTemp,MaxTemp of type int
int MaxTemp=-200;
int MinTemp=200;
if(key.find("Min")!=-1)
{
for(IntWritable i : values)
{
int temperature= i.get();
if(temperature<MinTemp)
MinTemp = temperature;
}
context.write(key, new IntWritable(MinTemp));
}
if(key.find("Max")!=-1)
{
for(IntWritable i : values)
{
int temperature= i.get();
if(MaxTemp<temperature)
MaxTemp = temperature;
}
context.write(key, new IntWritable(MaxTemp));
}
}
}
//Driver
/**
* @method main
* <p>This method is used for setting all the configuration properties.
* It acts as a driver for map reduce code.
* @return void
* @method_argumentsargs
* @throws Exception
*/
public static void main(String[] args) throws Exception {
//reads the default configuration of cluster from the configuration xml files
Configuration conf = new Configuration();
//Initializing the job with the default configuration of the cluster
Job job = Job.getInstance(conf,"MinMaxTemp");
//Assigning the driver class name
job.setJarByClass(MinMaxTemp.class);
//Defining the mapper class name
job.setMapperClass(MyMapper.class);
//Defining the reducer class name
job.setReducerClass(MyReducer.class);
//Defining the output key class for the final output i.e. from reducer
job.setOutputKeyClass(Text.class);
//Defining the output value class for the final output i.e. from reducer
job.setOutputValueClass(IntWritable.class);
//Defining input Format class which is responsible to parse the dataset into a key value pair
job.setInputFormatClass(TextInputFormat.class);
//Defining output Format class which is responsible to parse the final key-value output from MR framework to a text file into the hard disk
job.setOutputFormatClass(TextOutputFormat.class);
//setting the second argument as a path in a path variable
Path outputPath = new Path(args[1]);
job.setNumReduceTasks(1);
//Configuring the input/output path from the filesystem into the job
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
outputPath.getFileSystem(conf).delete(outputPath, true);
//exiting the job only if the flag value becomes false
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
