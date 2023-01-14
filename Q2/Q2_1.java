import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q2_1 extends Configured implements Tool{
	

	
	public static class RoutesMapper
     extends Mapper<LongWritable, Text, Text, IntWritable>{

private final static IntWritable one = new IntWritable(1);
String origin;
String destination;
String[] values;

 public void map(LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {
   
   values = value.toString().split(",");
   origin = values[17];
   destination = values[18];
   context.write(new Text(origin+destination), one);
   
 }
}


public static class RoutesReducer
    extends Reducer<Text,IntWritable,Text,LongWritable> {

 static long count;
 
 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
   
   count++;
   
 }
 
 
 public void cleanup(Context context) throws IOException, InterruptedException
 {
	 context.write(new Text("Number of unique flight routes"), new LongWritable(count));
 }
}

public int run(String[] args) throws Exception
{
	 Configuration conf = new Configuration();
	 conf.set("mapreduce.output.textoutputformat.separator",":");
	 Job job = Job.getInstance(conf, "unique routes count");
	 job.setJarByClass(Q2_1.class);
	 job.setMapperClass(RoutesMapper.class);
	 job.setReducerClass(RoutesReducer.class);
	 job.setNumReduceTasks(1);
	 job.setMapOutputKeyClass(Text.class);
	 job.setMapOutputValueClass(IntWritable.class);
	 job.setOutputFormatClass(TextOutputFormat.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(LongWritable.class);
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
	 return 0;
}



public static void main(String[] args) throws Exception {
	ToolRunner.run(new Configuration(), new Q2_1(),args);
    System.exit(0);
}
}
