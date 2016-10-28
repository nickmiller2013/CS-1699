// cc MaxTemperatureWithCombiner Application to find the maximum temperature, using a combiner function for efficiency
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// vv MaxTemperatureWithCombiner
public class MaxTemperatureWithCombiner {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCombiner <input path> " +
          "<output path>");
      System.exit(-1);
    }
    String output_path = args[1] + "_edit";
    
    Job job = new Job();
    job.setJarByClass(MaxTemperatureWithCombiner.class);
    job.setJobName("Max temperature");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(output_path));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    /*[*/job.setCombinerClass(MaxTemperatureReducer.class)/*]*/;
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.waitForCompletion(true);
    
    Job job2 = new Job();
    job2.setJarByClass(MaxTemperatureWithCombiner.class);
    job2.setJobName("Max temperature");

    FileInputFormat.addInputPath(job2, new Path(output_path));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    
    job2.setMapperClass(MaxTemperatureMapperAgain.class);
    /*[*/job2.setCombinerClass(MaxTemperatureReducerAgain.class)/*]*/;
    job2.setReducerClass(MaxTemperatureReducerAgain.class);

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);

    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
    
  }
}
// ^^ MaxTemperatureWithCombiner
