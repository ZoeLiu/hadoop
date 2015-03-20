package pat.citation.asterisk;

import java.lang.Math.*;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CitationAsterisk extends Configured implements Tool {
    
    public static class MapClass extends MapReduceBase
        implements Mapper<Text,Text,IntWritable,IntWritable> {
        
        public void map(Text key, Text value,
                        OutputCollector<IntWritable, IntWritable> output,
                        Reporter reporter) throws IOException {

            // convert text key to integer
            int iKey = Integer.parseInt(key.toString());
            // create a new key that groups the bucket. bucket size = 20
            int mult = (iKey - 1) / 20; 
           
            output.collect(new IntWritable(mult),new IntWritable(Integer.parseInt(value.toString())));
        
        }
    }
    
    public static class Reduce extends MapReduceBase
        implements Reducer<IntWritable,IntWritable,Text,Text> 
    {
        
        public void reduce(IntWritable key, Iterator<IntWritable> values,
                           OutputCollector<Text, Text>output, 
                           Reporter reporter) throws IOException {
                           
            int count = 0;
            
            while (values.hasNext()) {
                count += values.next().get();
            }

            // create label for output. e.g. 1-20 for original key range from 1 to 20 
            int startLabel = key.get() * 20 + 1;
            int endLabel = (key.get() + 1) * 20;
 
            // concatenate the start and end values as the label 
            String label = Integer.toString(startLabel).concat("-").concat(Integer.toString(endLabel));
 
            // create the 4log10(cite+1) and round it to integer
            int logCount = (int) Math.round(4 * Math.log10(count + 1));

            // create the asterisk based on the logCount
            String asteriskCount = "";
            for (int i = 0 ; i < logCount; i ++){
                  asteriskCount += "*";
            }    

            output.collect(new Text(label), new Text(asteriskCount));
        }
    }
    
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        JobConf job = new JobConf(conf, CitationAsterisk.class);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJobName("CitationAsterisk");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        JobClient.runJob(job);
        
        return 0;
    }
    
    public static void main(String[] args) throws Exception { 
        int res = ToolRunner.run(new Configuration(), 
                                 new CitationAsterisk(), 
                                 args);
        System.exit(res);
    }
}
