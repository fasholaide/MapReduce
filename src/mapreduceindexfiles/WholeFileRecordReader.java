/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreduceindexfiles;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ArrayWritable;

/**
 *
 * @author bofashola
 */
class WholeFileRecordReader implements RecordReader<Text, ArrayWritable> {

    private FileSplit fileSplit;
    private Configuration conf;
    private boolean processed = false;
    private ArrayWritable write = new ArrayWritable(new String[0]);

    public WholeFileRecordReader(FileSplit fileSplit, JobConf jc) {
        this.fileSplit = fileSplit;
        this.conf = jc;
    }

    @Override
    public boolean next(Text k, ArrayWritable v) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        //if (!processed) {
        //byte[] contents = new byte[(int) fileSplit.getLength()];
        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream in = null;
        try {
            in = fs.open(file);
            String linePosition = Long.toString(in.getPos());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            //IOUtils.readFully(in, contents, 0, contents.length);
            String line = "";
            while (reader.ready()) {
                line = reader.readLine();
            }
            Text values[] = new Text[2];
            values[0] = new Text(linePosition);
            values[1] = new Text(line);
            v.set(values);
            write = v;

        } finally {
            IOUtils.closeStream(in);
        }
        //processed = true;
        return true;
       // }
        //return false;

    }

    @Override
    public Text createKey() {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        Path file = fileSplit.getPath();
        String fileName = file.getName();
        return new Text(fileName);

    }

    @Override
    public ArrayWritable createValue() {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        String str[] = new String[0];
        return write;
    }

    @Override
    public long getPos() throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        return processed ? fileSplit.getLength() : 0;
    }

    @Override
    public void close() throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public float getProgress() throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        return processed ? 1.0f : 0.0f;
    }

}
