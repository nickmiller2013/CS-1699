// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    String output_path = args[1] + "_edit";
    
    Job job = new Job();
    job.setJarByClass(MaxTemperature.class);
    job.setJobName("Max temperature");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(output_path));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.waitForCompletion(true);
    
    Job job2 = new Job();
    job2.setJarByClass(MaxTemperature.class);
    job2.setJobName("Max temperature");

    FileInputFormat.addInputPath(job2, new Path(output_path));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    
    job2.setMapperClass(MaxTemperatureMapperAgain.class);
    job2.setReducerClass(MaxTemperatureReducerAgain.class);

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MaxTemperature
