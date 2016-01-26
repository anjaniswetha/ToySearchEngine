
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Writable;
import java.util.*;
import java.io.*; 
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.mapreduce.lib.output.*;

public class InvertedIndex {
	
	 public static List<String> fillerList = new ArrayList<String>();
	
	 public static class LineIndexMapper 
     extends Mapper<LongWritable, Text, Text, IntWritable>{

	    private final static Text word = new Text();
	    private final static Text location = new Text();

	    public void map(LongWritable key, Text val,
	    		Context context
                ) throws IOException, InterruptedException {

	      FileSplit fileSplit = (FileSplit)context.getInputSplit();
	      String fileName = fileSplit.getPath().getName();
	      location.set(fileName);

	      String line = val.toString().replaceAll("[^a-zA-Z]+"," ");
	      StringTokenizer itr = new StringTokenizer(line.toLowerCase());
	      while (itr.hasMoreTokens()) {
	    	  String words = itr.nextToken();
	    	  if(!fillerList.contains(words))
	    	  {
	    	  String keys =words +" "+location;
	          word.set(keys);
	          context.write(word, new IntWritable(1));
	    	  }
	      }
	    }
	  }
	 
	  public static class LineIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		  
		  public void reduce(Text key, Iterable<IntWritable> values, 
                  Context context
                  ) throws IOException, InterruptedException {
			  int count = 0;
			  int frequency = 0;
			  for (IntWritable val : values) {
				  frequency += val.get();
			   }

                 context.write(key, new IntWritable(frequency));
          }
     }
  	  
	  public static class CompositeKey implements Writable,
	  WritableComparable<CompositeKey> { 
		  	  
		  private String token ;
		  private String freq;
		  
		  public CompositeKey() {
		  }
		  
		  public CompositeKey(String token, String freq) {
			  this.token = token;
			  this.freq = freq;
		  }  
		  
		  public String toString() {
		   
		  return (new StringBuilder()).append(token).append(',').append(freq).toString();
		  }
		  
		  @Override
		  public void readFields(DataInput in) throws IOException {
		   
		  token = WritableUtils.readString(in);
		  freq = WritableUtils.readString(in);
		  }
		   
		  @Override
		  public void write(DataOutput out) throws IOException {
		   
		  WritableUtils.writeString(out, token);
		  WritableUtils.writeString(out, freq);
		  }
		  
		  
		  public int compareTo(CompositeKey o) {
		   
		  int result = token.compareTo(o.token);
		  if (0 == result) {
		     result = freq.compareTo(o.freq);
		  }
		     return result;
		  }
		  
		  public String getToken() {
			  
			  return token;
		  }
			   
		  public void setToken(String token) {
			   
			  this.token = token;
		   }
			  
		  public String getFreq() {
				  
			  return freq;
	       }
				   
		  public void setFreq(String freq) {
				   
				  this.freq = freq;
		   }
		 	  
	  }
	  
	  
	  
	  public static class ActualKeyPartitioner extends Partitioner<CompositeKey, Text> {
	   
	  HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<Text, Text>();
	  Text newKey = new Text();
	   
	  public int getPartition(CompositeKey key, Text value, int numReduceTasks) {
	   
	      try {
	       // Execute the default partitioner over the first part of the key  
	       newKey.set(key.getToken());
	       return hashPartitioner.getPartition(newKey, value, numReduceTasks);
	       } catch (Exception e) {
	          e.printStackTrace();
	           return (int) (Math.random() * numReduceTasks); // this would return a random value in the range
	  
	        }
	      }
	   }
	  public static class ActualKeyGroupingComparator extends WritableComparator {
		  
		  protected ActualKeyGroupingComparator() {
		   
		     super(CompositeKey.class, true);
		  }
		   
		  
		  public int compare(WritableComparable w1, WritableComparable w2) {
		   
		      CompositeKey key1 = (CompositeKey) w1;
		      CompositeKey key2 = (CompositeKey) w2;
		   
		  // (check on udid)
		      return key1.getToken().compareTo(key2.getToken());
		  }
	  }
	  
	  public static class CompositeKeyComparator extends WritableComparator {
		  protected CompositeKeyComparator() {
		     super(CompositeKey.class, true);
		  }
		  @SuppressWarnings("rawtypes")
		  @Override
		  public int compare(WritableComparable w1, WritableComparable w2) {
		   
		    CompositeKey key1 = (CompositeKey) w1;
		    CompositeKey key2 = (CompositeKey) w2;
		   
		  // (first check on token)
		    int compare = key1.getToken().compareTo(key2.getToken());
		   
		    if (compare == 0) {
		  // only if we are in the same input group should we try and sort by value (frequency)
		       return -1*Integer.compare(Integer.parseInt(key1.getFreq()),Integer.parseInt(key2.getFreq()));
		    }
		   
		     return compare;
		  }
	  }
	  
	  
	  
	  public static class SecondarySortMapper extends
	  Mapper<LongWritable, Text, CompositeKey, Text> {
		  
		  public SecondarySortMapper(){
			  
		  }
		  Text values = new Text();
	
	       public void map(LongWritable key, Text value, Context context)
	          throws IOException, InterruptedException {
	           String token = "";
	            String docid = "";
	            String freq = "";
		  
	           if (value.toString().length() > 0) {
		          StringTokenizer itr = new StringTokenizer(value.toString());
		  
		          while(itr.hasMoreTokens() && itr.countTokens() == 3)
		           {
			           token = itr.nextToken();
			           docid = itr.nextToken();
			           freq = itr.nextToken();
		            }
	   
		           String val = docid+":"+freq;
		           values.set(val);
	               context.write( new CompositeKey(token,freq), values);
	            }
	   
	        }
	  } 
	  
	 
	  
	  
   public static class SecondarySortReducer
    extends
      Reducer<CompositeKey, Text, Text, Text> {
	
	   private MultipleOutputs<Text, Text> out;
	 
		public void setup(Context context) 
		{
			out = new MultipleOutputs<Text, Text>(context);
		}
	 
		 Text newKey1 = new Text();
	     Text values1 = new Text();
	  
	

       public void reduce(CompositeKey key, Iterable<Text> values,
       Context context) throws IOException, InterruptedException {
 
           	String reducerValues = "";
	        String s = "";
	
	        for (Text val : values) {
		       s = val.toString().trim();
		       reducerValues = reducerValues + " " + s;
	        }

	       String keys =  key.getToken().toString();
	       newKey1.set(keys);
	       values1.set(reducerValues);
	//context.write(newKey1, values1);
	       out.write(newKey1, values1, generateFileName(newKey1));
       }
       private String generateFileName(Text k) 
       {
	      String key=k.toString();
		  char f=key.charAt(0);
	      String x=f+"-WORD";
	      return x;
        }
       protected void cleanup(Context context) throws IOException, InterruptedException 
      {
	    out.close();
      }
   } 
	  
	  public static void main(String[] args) throws Exception{
		  Configuration conf = new Configuration();
		  String[] otherArgs = new GenericOptionsParser(conf, args)
			.getRemainingArgs();
	if (otherArgs.length != 3) {
		System.err.println("Usage: Inverted Index <in> <temp> <out>");
		System.exit(2);
	}
	      
	        fillerList =  readFillerFile();
	

		    Job job1 = new Job(conf, "Inverted Index");
		    job1.setJarByClass(InvertedIndex.class);
		    job1.setMapperClass(LineIndexMapper.class);
		    //job.setCombinerClass(IntSumReducer.class);
		    job1.setReducerClass(LineIndexReducer.class);

		    job1.setInputFormatClass (TextInputFormat.class);
		    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));

		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(IntWritable.class);
              
		    job1.waitForCompletion(true);
		    
		    Job job2 = new Job(conf, "Inverted Index");
			job2.setJarByClass(InvertedIndex.class);
			job2.setMapperClass(SecondarySortMapper.class);
			job2.setMapOutputKeyClass(CompositeKey.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setPartitionerClass(ActualKeyPartitioner.class);
			job2.setSortComparatorClass(CompositeKeyComparator.class);
			job2.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
			job2.setReducerClass(SecondarySortReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class); 
		
			FileInputFormat.addInputPath(job2, new Path(otherArgs[1]
					+ "part-r-00000"));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
	      
		
	  }
	  
	public static  ArrayList<String> readFillerFile() throws Exception
	  {
		  String token1 = "";
		  FileReader in = new FileReader("/home/cosc6376/bigd24/input/fillerword.txt");
		    BufferedReader br = new BufferedReader(in);
		  
		  
		  ArrayList<String> temps = new ArrayList<String>();
		  while (br.readLine() != null) {
			  token1 = br.readLine();
		      temps.add(token1);
		  }
		
		  in.close();
		  return temps;
     }
	  
}
