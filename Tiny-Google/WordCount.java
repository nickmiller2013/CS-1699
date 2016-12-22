import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import java.util.Date;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import java.util.Collections;
import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.Iterator;

public class WordCount {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IIElement>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      IIElement element = new IIElement();
      Text word = new Text();
      StringTokenizer itr = new StringTokenizer(value.toString());
	    String regex = "[][(){},.;!?<>%]";
	
	    //output of map is word mapped to a IIElement, which contains filename and frequency of word
      while (itr.hasMoreTokens()) {
        String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
	      word.set(itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z ]",""));
        element.setFilename(fileName);
        element.setFreq(1); 
	      context.write(word, element);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,IIElement,Text,Text> {
    public void reduce(Text key, Iterable<IIElement> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      String previousFile = "";
      String currentFile = "";
      ArrayList<IIElement> ii = new ArrayList<>();
      Iterator iter = values.iterator();
      
      while(true) {
        IIElement val = (IIElement)iter.next();
        currentFile = val.getFilename();
        
        if (sum == 0) {
          sum++;
          if (iter.hasNext()) {
            previousFile = currentFile;
            continue;
          }
          else {
            IIElement newElement = new IIElement();
            newElement.setFreq(sum);
            newElement.setFilename(currentFile);
            ii.add(newElement);
            break;
          }
        }
        
        if (!currentFile.equals(previousFile)) {
          IIElement newElement = new IIElement();
          newElement.setFreq(sum);
          newElement.setFilename(previousFile);
          ii.add(newElement);
          sum = 1;
        } else {
          sum++;
        }
        
        previousFile = currentFile;
        
        if (!iter.hasNext()) {
          IIElement newElement = new IIElement();
          newElement.setFreq(sum);
          newElement.setFilename(previousFile);
          ii.add(newElement);
          break;
        }
      }
      
      Collections.sort(ii);
      Collections.reverse(ii);
      StringBuilder line = new StringBuilder();
      for (IIElement element : ii) {
        line.append(element.toString());
      }
      
      context.write(key, new Text(line.toString()));
    }
  }
  
  /*
   * Custome object to store each element in our inverted Index
   */
  public static class IIElement implements WritableComparable<IIElement> {
    private String filename;
    private int freq;
    
    public void setFilename(String filename) {
      this.filename = filename;
    }
    
    public void setFreq(int freq) {
      this.freq = freq;
    }
    
    public int getFreq() {
      return freq;
    }
    
    public String getFilename() {
      return filename;
    }
    
    public void write(DataOutput out) throws IOException {
      out.writeUTF(filename);
      out.writeInt(freq);
    }
    
    public void readFields(DataInput in) throws IOException {
      filename = in.readUTF();
      freq = in.readInt();
    }
    
    /*
     * Compares two elements in the Inverted Index
     *
     * @param o the element we want to compare this element to
     * @return postive int if this object is greater than object being passed, negative int if
     * less than, and zero is two objects are equals
     */
    public int compareTo(IIElement o) {
      int thisValue = this.freq;
      int thatValue = o.freq;
      return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
    
    public String toString() {
      return filename + " " + freq + " ";
    }
    
  }
  
  public static void main(String[] args) throws Exception {
    Date date = new Date();
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IIElement.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    long start = new Date().getTime(); 
    boolean status = job.waitForCompletion(true); 
    long end = new Date().getTime(); 
    System.out.println("Job took "+(end-start) + "milliseconds");
    
  }
}
