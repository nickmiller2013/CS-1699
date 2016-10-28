// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapperAgain
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = 9999;
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    String temp[] = line.split("\t");
    String tempr = temp[1];
    int tempT = Integer.parseInt(tempr);
    String keyout = line.substring(0,10) + "-" + line.substring(15,19);
    //String identifier = line.substring(26);
    //int id = Integer.parseInt(identifier);
    context.write(new Text(keyout), new IntWritable(tempT));
  }
}
// ^^ MaxTemperatureMapper
