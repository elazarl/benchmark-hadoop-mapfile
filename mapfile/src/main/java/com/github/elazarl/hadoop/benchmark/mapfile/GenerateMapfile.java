package com.github.elazarl.hadoop.benchmark.mapfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Generates random mapfile
 */
public class GenerateMapfile {

    public static MapFile.Writer create(Configuration conf, Path p) throws IOException {
        return new MapFile.Writer(conf, p, MapFile.Writer.keyClass(LongWritable.class),
                MapFile.Writer.valueClass(Text.class));
    }
    public static void write(MapFile.Writer mapw, long size) throws IOException {
        byte[] buf = new byte[Pairs.MAX_LEN];
        Text t = new Text();
        LongWritable ixw = new LongWritable();
        for (long ix = 0; ix < size; ix ++) {
            int len = Pairs.fillOfIndex(ix, buf);
            t.set(buf, 0, len);
            ixw.set(ix);
            mapw.append(ixw, t);
        }
        mapw.close();
    }
}
