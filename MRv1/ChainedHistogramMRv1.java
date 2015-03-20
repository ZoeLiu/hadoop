package pat.chained.mrv1;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class ChainedHistogramMRv1 extends Configured implements Tool {


/*********************************
   Job1: Patents list by country
**********************************/

    public static class MapClass1 extends MapReduceBase
        implements Mapper<Text,Text,Text,Text> {
        public void map(Text key, Text value, 
                        OutputCollector<Text,Text> output,
                        Reporter reporter 
                        ) throws IOException {
        String line = value.toString();
        String[] tokens = line.split(",");
        if (tokens.length >= 5){ 
            String country = tokens[3] + tokens[4];    
            output.collect(new Text(country),key);
        }
       }
    }

    public static class Reduce1 extends MapReduceBase
        implements Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text,Text> output,
                           Reporter reporter
                           ) throws IOException {
            String csv ="";
            //use StringBuilder to optimize concatenation  
            StringBuilder sb = new StringBuilder(csv);

            while (values.hasNext()) {

                if (sb.length()>0) sb.append(",");
                sb.append(values.next().toString());
            }

            csv = sb.toString();
            output.collect(key, new Text(csv));
         }
    }

    private JobConf createJob1(Configuration conf, Path in, Path out) 
            throws Exception {
        
        JobConf job = new JobConf(conf, ChainedHistogramMRv1.class);
	job.setJobName("job1");
        job.set("key.value.separator.in.input.line",",");

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(MapClass1.class);
        job.setReducerClass(Reduce1.class);
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
       

        return job;
     }
  


/*********************************
   Job2: Patent Count by Country
**********************************/

    
    public static class MapClass2 extends MapReduceBase
        implements Mapper<Text,Text,Text,Text> {
        public void map(Text key, Text value,
                        OutputCollector<Text,Text> output,
                        Reporter reporter 
                       ) throws IOException {

            output.collect(key,value);
        }
       }
   

    public static class Reduce2 extends MapReduceBase
        implements Reducer<Text,Text,Text,IntWritable> {
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text,IntWritable> output,
                           Reporter reporter
                           ) throws IOException {

            int count = 0;
             
            while (values.hasNext()) {
                count += values.next().toString().split(",").length;
            }

            output.collect(key, new IntWritable(count));
         }
    }


    private JobConf createJob2(Configuration conf, Path in, Path out) 
            throws Exception {

        JobConf job = new JobConf(conf, ChainedHistogramMRv1.class);
        job.setJobName("job2");
        job.set("key.value.separator.in.input.line","\t");

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(MapClass2.class);
        job.setReducerClass(Reduce2.class);  
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //set the output class for the map
        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(Text.class); 

        return job;
    }



/****************************************
       Run Jobs
*****************************************/

    public void cleanup(Path temp, Configuration conf) throws Exception {
        FileSystem fs = temp.getFileSystem(conf);    
        fs.delete(temp,true);
    }


    public int run(String[] args) throws Exception {
        
        Configuration conf = getConf();

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        Path temp = new Path("chain-temp");

        JobConf job1 = createJob1(conf, in, temp);
        JobClient.runJob(job1);
        
        JobConf job2 = createJob2(conf, temp, out);
        JobClient.runJob(job2);
        
        cleanup(temp,conf);

        return 0;
     }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                                 new ChainedHistogramMRv1(),
                                 args);
        System.exit(res);
    }
 
}

