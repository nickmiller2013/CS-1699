// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
      int sum = 0;
      int count = 0;
      int maxValue = Integer.MIN_VALUE;
      for (IntWritable val : values) {
        sum = Math.max(maxValue, val.get()); 
     }

      String outkey = key.toString();
      context.write(new Text(outkey), new IntWritable(sum));
  }
}
// ^^ MaxTemperatureReducer
