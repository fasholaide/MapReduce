/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreduceindexfiles;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 *
 * @author bofashola
 */
public class WholeFileInputFormat extends FileInputFormat<Text, ArrayWritable> {

    @Override
    public RecordReader<Text, ArrayWritable> getRecordReader(InputSplit is, JobConf jc, Reporter rprtr) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        return new WholeFileRecordReader((FileSplit) is, jc);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return true;
    }

}
