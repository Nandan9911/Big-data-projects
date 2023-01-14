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

public class Q3_1 extends Configured implements Tool{
	

	
public static class MaxCenturiesMapper
     extends Mapper<LongWritable, Text, Text, IntWritable>{

private final static IntWritable one = new IntWritable(1);
String player;
int runs;
String[] values;

 public void map(LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {
   
   values = value.toString().split(",");
   player = values[1];
   runs = Integer.parseInt(values[2]);
   
   if(runs>=100)
       context.write(new Text(player),one);
   }
}


public static class MaxCenturiesReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {

 
 static int max;
 static String playerName;
 
 
 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
   
   int count=0;
   for(IntWritable i:values)
   {
	  count += i.get();
   }
   
   if(count>max)
   {
	   max=count;
	   playerName=key.toString();
   }
   
 }
 
 
 public void cleanup(Context context) throws IOException, InterruptedException
 {
	 context.write(new Text(playerName), new IntWritable(max));
 }
 
}

public int run(String[] args) throws Exception
{
	 Configuration conf = new Configuration();
	 conf.set("mapreduce.output.textoutputformat.separator",":");
	 Job job = Job.getInstance(conf, "maximum centuries by player");
	 job.setJarByClass(Q3_1.class);
	 job.setMapperClass(MaxCenturiesMapper.class);
	 job.setReducerClass(MaxCenturiesReducer.class);
	 job.setNumReduceTasks(1);
	 job.setMapOutputKeyClass(Text.class);
	 job.setMapOutputValueClass(IntWritable.class);
	 job.setOutputFormatClass(TextOutputFormat.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(IntWritable.class);
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
	 return 0;
}



public static void main(String[] args) throws Exception {
	ToolRunner.run(new Configuration(), new Q3_1(),args);
    System.exit(0);
}
}
