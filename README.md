# Hadoop MapFile benchmark

## Goal

This repository contains a simple benchmark for comparing the throughput one would get from
using plain [MapFile](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/MapFile.html)
to the throughput one would get from a typical key-value distributed database.

Obviously, using plain file will not be as fast by using a dedicated key-value service, but how slower
would that be?  We compare the throughput of insertions and random lookup with insertions and random lookup to HBase.

Results for [MapFile](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/MapFile.html)
seems to be in line with [builtin benchmarks](http://wiki.apache.org/hadoop/Hbase/NewFileFormat/Performance).

## Results

See [output for HBase](hbasebench.txt) and [output for MapFile](mapfilebench.txt).

### Insertions

[MapFile](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/MapFile.html) is slower (by ~20%) than
HBase's insertion rate, when HBase's client batches insertions, i.e.
[`setAutoFlush`](http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/HTable.html#setAutoFlush(boolean))`(false)`.

With `setAutoFlush` on, HBase is very slow, it gives ~1% of the throughput.

HBase guarantees rows will be synced to the disk after a successful `Put`.
I'm not sure which guarantees does
[`MapFile.Writer.append`](http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/io/MapFile.Writer.html#append(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.Writable))
gives you (will the data be written to a file at all? will the filesystem be `sync`'d with the new row).

### Lookup

On uniformly random lookup of 1% from 100 million records, HBase is much faster (~×100 more throughput) than plain 
[MapFile](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/MapFile.html)
even though most of the records are uncached.

## Batching `Get`s in HBase

As a side note, it was found that batching `Get`s highly affects the throughput of HBase by at least an order of mangitude (~×20 higher throughput).
Even then, HBase was still searching faster than a MapFile. See [results](hbasebench_buckets.txt).

## Conclusion

In principle, static
[MapFile](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/MapFile.html)s
could be used as an ad-hoc database.
They're not much different than HBase's HFile internal mechanism, and IMHO with a proper user library can achieve simlar results.
However one cannot rely on the current implementation of
[MapFile](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/MapFile.html)
for fast random lookups.

## Running the Benchmark

Build it from the main project;

    $ git clone https://github.com/elazarl/benchmark-hadoop-mapfile.git
    $ cd benchmark-hadoop-markfile
    $ mvn clean package

then, on a machine with hadoop and hbase installed and configured:

    $ # insert 10,000,000 lines, perform 10 times lookup of 1% (= 100,000)
    $ java -cp `hbase classpath`:hbase/target/mapfile-*.jar com.github.elazarl.hadoop.benchmark.mapfile.HBaseMain -n 10m 2>/dev/null
    $ # same as above, but perform one time lookup of 1%
    $ java -cp `hbase classpath`:hbase/target/mapfile-*.jar com.github.elazarl.hadoop.benchmark.mapfile.HBaseMain -iteration 1 -n 10m 2>/dev/null
    $ # same as above, but use auto flush
    $ java -cp `hbase classpath`:hbase/target/mapfile-*.jar com.github.elazarl.hadoop.benchmark.mapfile.HBaseMain -autoflush 1 -n 10m 2>/dev/null
    $ # same as above, but use 10 get buckets for random search, instead of default 1000
    $ java -cp `hbase classpath`:hbase/target/mapfile-*.jar com.github.elazarl.hadoop.benchmark.mapfile.HBaseMain -bucket 10 -n 10m 2>/dev/null

to measure the same with hadoop, we assume the hadoop executable is in path:
[MapFile](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/MapFile.html):

    $ hadoop jar mapfile/target/mapfile-*.jar -n 10m
    $ hadoop jar mapfile/target/mapfile-*.jar -iteration 1 -n 10m

results are written to standard output.
