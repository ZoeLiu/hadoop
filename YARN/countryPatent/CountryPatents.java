package pat;
import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class CountryPatents extends Configured implements Tool {
    public static class MapClass extends
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

    public static class Reduce extends
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

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        Job job = Job.getInstance(conf,"Country Patents");
        /*Job job = new job(conf,"Country Patents"); 
         - this is deprecated in Hadoop >2.2 versions*/
        job.setJarByClass(CountryPatents.class);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setJobName("CountryPatents");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true)?0:1);
        return 0;
     }
  
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                                 new CountryPatents(),
                                 args);
        System.exit(res);
}
}

