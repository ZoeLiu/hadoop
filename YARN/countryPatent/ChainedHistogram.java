package pat.chained;
import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class ChainedHistogram extends Configured implements Tool {


/*********************************
   Job1: Patents list by country
**********************************/

    public static class MapClass1 extends
        Mapper<Text,Text,Text,Text> {
        public void map(Text key, Text value,Context context 
                       ) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");
        if (tokens.length >= 5){ 
            String country = tokens[3] + tokens[4];    
            context.write(new Text(country),key);
        }
       }
    }

    public static class Reduce1 extends
        Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,Context context
                           ) throws IOException, InterruptedException {
            String csv ="";
            //use StringBuilder to optimize concatenation  
            StringBuilder sb = new StringBuilder(csv);
            for (Text patent:values) {
               /* if (csv.length()>0) csv += ",";
                  csv += patent.toString();*/
                if (sb.length()>0) sb.append(",");
                sb.append(patent);
            }
            csv = sb.toString();
            context.write(key, new Text(csv));
         }
    }

    private Job createJob1(Configuration conf, Path in, Path out) 
            throws Exception {

        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        Job job = Job.getInstance(conf,"job1");
        job.setJarByClass(ChainedHistogram.class);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(MapClass1.class);
        job.setReducerClass(Reduce1.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
       
        return job;
     }
  


/*********************************
   Job2: Patent Count by Country
**********************************/

    
    public static class MapClass2 extends
        Mapper<Text,Text,Text,Text> {
        public void map(Text key, Text value,Context context 
                       ) throws IOException, InterruptedException {

            context.write(key,value);
        }
       }
   

    public static class Reduce2 extends
        Reducer<Text,Text,Text,IntWritable> {
        public void reduce(Text key, Iterable<Text> values,Context context
                           ) throws IOException, InterruptedException {

            int count = 0;
           
            for (Text patent:values) {
                 count += patent.toString().split(",").length;
            }
            context.write(key, new IntWritable(count));
         }
    }


    private Job createJob2(Configuration conf, Path in, Path out) 
            throws Exception {

        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator","\t"); 
        Job job = Job.getInstance(conf,"job2");
        job.setJarByClass(ChainedHistogram.class);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(MapClass2.class);
        job.setReducerClass(Reduce2.class);  
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
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

        Job job1 = createJob1(conf, in, temp);
        job1.waitForCompletion(true);
        
        Job job2 = createJob2(conf, temp, out);
        job2.waitForCompletion(true);
        
        cleanup(temp,conf);

        return 0;
     }
 

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                                 new ChainedHistogram(),
                                 args);
        System.exit(res);
    }

}
