import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AllTimeHigh{
	
	
  	public static class StockMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
  		
  		String stockSym;
  		double highPrice;
  		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  		{
  			String[] str = value.toString().split(",");
  			stockSym = str[1];
  			highPrice = Double.parseDouble(str[4]);
  			context.write(new Text(stockSym),new DoubleWritable(highPrice));
  			
  		}
 

 }

  public static class StockReducer  extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	  
	  public void reduce(Text stockSym,Iterable<DoubleWritable> highValues, Context context)throws IOException, InterruptedException
	  {
		  double maxPrice=0;
		  
		  for(DoubleWritable p:highValues)
		  {
			  if(p.get()>maxPrice)
				  maxPrice=p.get();
		  }
		  
		  context.write(stockSym,new DoubleWritable(maxPrice));
	  }
   
  }

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Stock All Time High Price ");
    job.setJarByClass(AllTimeHigh.class);
    job.setMapperClass(StockMapper.class);
    job.setReducerClass(StockReducer.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

