import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q1_1 extends Configured implements Tool{
	

	
	public static class ShakespeareMapper
     extends Mapper<LongWritable, Text, Text, IntWritable>{

private final static IntWritable one = new IntWritable(1);
private Text word = new Text();
static Set<String> s = new HashSet<>();

 public void map(LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString());
   int len;
   Pattern pattern;
   Matcher matcher;
   while (itr.hasMoreTokens()) {
 	  String myword = itr.nextToken().toLowerCase();
 	  len = myword.length();
 	  pattern = Pattern.compile("[a-z']{1,"+len+"}");
 	  matcher = pattern.matcher(myword);
 	  if(matcher.find())
 	  {
 		  myword = matcher.group();
 		  word.set(myword);
 	      context.write(word, one);
 	  }
 	}
 }
}


	
	
public static class IntSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {

 static int max;
 static long count;
 static String k;
 
 public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
   int sum = 0;
   for (IntWritable val : values) {
     sum += val.get();
   }
   
   /*Configuration conf = context.getConfiguration();
   Cluster cluster = new Cluster(conf);
   Job currentJob = cluster.getJob(context.getJobID());
   long reduceSize = currentJob.getCounters().findCounter(TaskCounter.REDUCE_INPUT_GROUPS).getValue();*/
   
   //long reduceSize = context.getConfiguration().getLong("numUniqueWords", -1);
   
   if(sum>max)
   {
	   max = sum;
	   k = key.toString();
   }
   
 }
 
 public void cleanup(Context context) throws IOException, InterruptedException
 {
	 context.write(new Text(k), new IntWritable(max));
 }
}

public int run(String[] args) throws Exception
{
	 Configuration conf = new Configuration();
	 //conf.setLong("numUniqueWords", 0);
	 Job job = Job.getInstance(conf, "word count shakespeare");
	 job.setJarByClass(Q1_1.class);
	 job.setMapperClass(ShakespeareMapper.class);
	 job.setReducerClass(IntSumReducer.class);
	 job.setNumReduceTasks(1);
	 job.setMapOutputKeyClass(Text.class);
	 job.setMapOutputValueClass(IntWritable.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(IntWritable.class);
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
	 return 0;
}



public static void main(String[] args) throws Exception {
	ToolRunner.run(new Configuration(), new Q1_1(),args);
    System.exit(0);
}
}
