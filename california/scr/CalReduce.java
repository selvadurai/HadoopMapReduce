import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class CalReduce {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	
	
    private Text City  = new Text();
    private final static IntWritable One = new IntWritable(1);
    
    //MapReduce Looking For California Cities.
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { //Start of MapReduce. 
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line,",");
        
        while (tokenizer.hasMoreTokens()) {
        
        	String  state = tokenizer.nextElement().toString();
        	Integer num  = Integer.parseInt(tokenizer.nextElement().toString());
        	String  city = tokenizer.nextElement().toString(); 
        	Integer num1  = Integer.parseInt(tokenizer.nextElement().toString());
        	String  county = tokenizer.nextElement().toString(); 
        	String  lat = tokenizer.nextElement().toString(); 
        	String  log = tokenizer.nextElement().toString();
        	
            if(state.contentEquals("CA")){
               City.set(city);
               context.write(City,One);
             }
         } 
      
       }
    }//end of MapReduce class
    
    //Start of Reduce Class
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	 public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
    		        int sum = 0;
    		        for (IntWritable val : values) {
    		            sum += val.get();
    		        }
    		        context.write(key, new IntWritable(sum));
         }
     }//End of Reduce Class. 
    
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
            
        @SuppressWarnings("deprecation")
    	Job job = new Job(conf, "CalReduce");
        
        job.setJarByClass(CalReduce.class); 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
            
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
            
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
            
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
            
        job.waitForCompletion(true);
     
      
	} 
} //end of CalReduce 
	
	
	
	

