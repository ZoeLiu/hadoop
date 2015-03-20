package riemann.sum;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
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

public class RiemannSum extends Configured implements Tool {
    
    public static class MapClass extends MapReduceBase
        implements Mapper<Text, Text, Text, DoubleWritable> {
        
        public void map(Text key, Text value,
                        OutputCollector<Text, DoubleWritable> output,
                        Reporter reporter) throws IOException {
            
            // define delta x (size of increment on x)
            double delta = (10.0 - 1.0)/(10000 - 1);
            // define each x
            double xi = ((double) Integer.parseInt(key.toString())-1)*(10 - 1)/(10000 - 1) + 1;
            // calculate the area of rectangle
            double si = 1.0/xi * delta;

            // new key
            String RS = "Riemann Sum:";
 
            output.collect(new Text(RS), new DoubleWritable(si));
        }
    }   
    public static class Reduce extends MapReduceBase
        implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        
        public void reduce(Text key, Iterator<DoubleWritable> values,
                           OutputCollector<Text, DoubleWritable> output,
                           Reporter reporter) throws IOException {
                           
            double sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new DoubleWritable(sum));
        }
    }   
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();      
        JobConf job = new JobConf(conf, RiemannSum.class);    
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJobName("RiemannSum");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);   
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);     
        JobClient.runJob(job);      
        return 0;
    }  
    public static void main(String[] args) throws Exception { 
        int res = ToolRunner.run(new Configuration(), new RiemannSum(), args);       
        System.exit(res);
    }
}
