package com.github.elazarl.hadoop.benchmark.mapfile;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

/**
 * Writes key-value pairs to HBase, for benchmarking purposes.
 */
public class GenerateToHbase {

    public static final byte[] family = Bytes.toBytes("f");
    /**
     * {@code write} would write {@code size} amount of rows to {@code table},
     * for benchmarking purposes. We assume autoflush is off in the table, else
     * performance will be slow.
     * @param table table
     */
    public static void write(HTableInterface table, long size) throws IOException {
        byte[] buf = new byte[Pairs.MAX_LEN];
        for (long ix = 0; ix < size; ix++) {
            int len = Pairs.fillOfIndex(ix, buf);
            byte[] data = Arrays.copyOfRange(buf, 0, len);
            table.put(new Put(Bytes.toBytes(ix)).add(
                    family, null, data
            ));
        }
    }
}
