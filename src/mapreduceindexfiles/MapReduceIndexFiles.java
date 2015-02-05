/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreduceindexfiles;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author bofashola
 */
public class MapReduceIndexFiles {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        if (args.length != 3) {
            System.out.print(args.length);
            System.err.println("Usage: MapReduceIndexFiles <input> <output> <sequence>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("sequence", args[2]);
        //conf.set("mapred.child.java.opts", "-Xmx1000m");
        //conf.set("mapred.reduce.child.java.opts", "-Xmx1g");
        //conf.set("mapred.map.child.java.opts", "-Xmx1g");
        // conf.set("mapred.tasktracker.reduce.tasks.maximum", "10");
        //conf.set("mapred.tasktracker.map.tasks.maximum", "10");
        //conf.set("io.sort.mb", "4000");
        JobConf job = new JobConf(conf);
        job.setJarByClass(MapReduceIndexFiles.class);

        WholeFileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //job.setCombinerClass(IndexFilesReducer.class);
        //job.setInputFormat(WholeFileInputFormat.class);
        job.setMapperClass(IndexFilesMapper.class);
        job.setReducerClass(IndexFilesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Text.class);
        //job.setInputFormat(FileInputFormat.class);
        //job.setOutputFormat(FileOutputFormat.class);
        //job.setNumMapTasks(10);
        //job.setNumReduceTasks(10);
        Date start = new Date();
        JobClient.runJob(job);
        Date end = new Date();
        System.out.println("The Process took " + (end.getTime() - start.getTime()) / 1000 + "seconds");
    }

    static class IndexFilesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>, JobConfigurable {

        private String sequence = "";

        @Override
        public void configure(JobConf job) {
            sequence = (job.get("sequence"));
        }

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
            String filename = fileSplit.getPath().getName();
            if (value.toString().toLowerCase().contains(sequence.toLowerCase())) {
                final byte[] utf8Bytes = value.toString().getBytes("UTF-16");
                long lineNumber = (key.get()/utf8Bytes.length) * 2;
                String v = lineNumber +" +/- 4" + ":\t" + value.toString().toLowerCase().indexOf(sequence.toLowerCase());
                output.collect(new Text(filename), new Text(v));
            }

        }
    }

    static class IndexFilesReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>, JobConfigurable {

        private String heapSize = "";

        public IndexFilesReducer() {

        }

        @Override
        public void configure(JobConf job) {
            heapSize = job.get("mapred.child.java.opts");
        }

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
            String str = "\n";
            while (values.hasNext()) {
                Text value = values.next();
                str +=  value.toString()+ "\n";

            }
            collector.collect(key, new Text(str));

        }

    }

}
