![Common Crawl Logo](http://commoncrawl.org/wp-content/uploads/2016/12/logocommoncrawl.png)

# Common Crawl PySpark Examples

This project provides examples how to process the Common Crawl dataset with [Apache Spark](http://spark.apache.org/) and Python:

+ [count HTML tags](./html_tag_count.py) in Common Crawl's raw response data (WARC files)
+ [count web server names](./server_count.py) in Common Crawl's metadata (WAT files or WARC files)
+ list host names and corresponding [IP addresses](./server_ip_address.py) (WAT files or WARC files)
+ [word count](./word_count.py) (term and document frequency) in Common Crawl's extracted text (WET files)
+ [extract links](./wat_extract_links.py) from WAT files and [construct the (host-level) web graph](./hostlinks_to_graph.py) – for further details about the web graphs see the project [cc-webgraph](/commoncrawl/cc-webgraph)
+ work with the [columnar URL index](http://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/) (cf. [cc-index-table](/commmoncrawl/cc-index-table)):
  - run a SQL query and [export the result as a table](./cc_index_export.py)
  - select WARC records by a SQL query, parse the HTML, extract the text and [counts word](./cc_index_word_count.py)


## Setup

To develop and test locally, you will need to install
* Spark, see the [detailed instructions](http://spark.apache.org/docs/latest/), and
* all required Python modules by running
```
pip install -r requirements.txt
```

## Compatibility and Requirements

Tested with Spark 2.1.0 - 2.4.0 in combination with Python 2.7 or 3.5 and 3.6.


## Get Sample Data

To develop locally, you'll need at least three data files -- one for each format the crawl uses. They can be fetched from the following links:
* [warc.gz](https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/warc/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.gz)
* [wat.gz](https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/wat/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.wat.gz)
* [wet.gz](https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/wet/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.wet.gz)

Alternatively, running `get-data.sh` will download the sample data. It also writes input files containing
* sample input as `file://` URLs
* all input of one monthly crawl as `s3://` URLs


### Running locally

First, point the environment variable `SPARK_HOME` to your Spark installation. 
Then submit a job via

```
$SPARK_HOME/bin/spark-submit ./server_count.py \
	--num_output_partitions 1 --log_level WARN \
	./input/test_warc.txt servernames
```

This will count web server names sent in HTTP response headers for the sample WARC input and store the resulting counts in the SparkSQL table "servernames" in your ... (usually in `./spark-warehouse/servernames`). The 

The output table can be accessed via SparkSQL, e.g.,

```
$SPARK_HOME/spark/bin/pyspark
>>> df = sqlContext.read.parquet("spark-warehouse/servernames")
>>> for row in df.sort(df.val.desc()).take(10): print(row)
... 
Row(key=u'Apache', val=9396)
Row(key=u'nginx', val=4339)
Row(key=u'Microsoft-IIS/7.5', val=3635)
Row(key=u'(no server in HTTP header)', val=3188)
Row(key=u'cloudflare-nginx', val=2743)
Row(key=u'Microsoft-IIS/8.5', val=1459)
Row(key=u'Microsoft-IIS/6.0', val=1324)
Row(key=u'GSE', val=886)
Row(key=u'Apache/2.2.15 (CentOS)', val=827)
Row(key=u'Apache-Coyote/1.1', val=790)
```

See also
* [running the Spark shell and submitting Spark jobs](http://spark.apache.org/docs/latest/#running-the-examples-and-shell)
* [PySpark SQL API](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html)


### Running in Spark cluster over large amounts of data

As the Common Crawl dataset lives in the Amazon Public Datasets program, you can access and process it on Amazon AWS without incurring any transfer costs. The only cost that you incur is the cost of the machines running your Spark cluster.

1. spinning up the Spark cluster: [AWS EMR](https://aws.amazon.com/documentation/emr/) contains a ready-to-use Spark installation but you'll find multiple descriptions on the web how to deploy Spark on a cheap cluster of AWS spot instances. See also [launching Spark on a cluster](http://spark.apache.org/docs/latest/#launching-on-a-cluster).

2. choose appropriate cluster-specific settings when [submitting jobs](http://spark.apache.org/docs/latest/submitting-applications.html) and also check for relevant command-line options (e.g., `--num_input_partitions` or `--num_output_partitions`) by running 

  ./spark/bin/spark-submit ./server_count.py --help

3. don't forget to deploy all dependencies in the cluster, see [advanced dependency management](http://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management)

4. also the the file `sparkcc.py` needs to be deployed or added as argument `--py-files sparkcc.py` to `spark-submit`. Note: some of the examples require further Python files as dependencies.


### Command-line options

All examples show the available command-line options if called with the parameter `--help` or `-h`, e.g.
```
$SPARK_HOME/bin/spark-submit ./server_count.py --help
```


## Credits

Examples are originally ported from Stephen Merity's [cc-mrjob](//github.com/commoncrawl/cc-mrjob/) with the following changes and upgrades:
* based on Apache Spark (instead of [mrjob](https://pythonhosted.org/mrjob/))
* [boto3](http://boto3.readthedocs.io/) supporting multi-part download of data from S3
* [warcio](https://github.com/webrecorder/warcio) a Python 2 and Python 3 compatible module to access WARC files

Further inspirations are taken from
* [cosr-back](//github.com/commonsearch/cosr-back) written by Sylvain Zimmer for [Common Search](https://about.commonsearch.org/). You definitely should have a look at it if you need a more sophisticated WARC processor (including a HTML parser for example).
* Mark Litwintschik's blog post [Analysing Petabytes of Websites](http://tech.marksblogg.com/petabytes-of-website-data-spark-emr.html)


## License

MIT License, as per [LICENSE](./LICENSE)
