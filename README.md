![Common Crawl Logo](https://commoncrawl.org/wp-content/uploads/2016/12/logocommoncrawl.png)

# Common Crawl PySpark Examples

This project provides examples how to process the Common Crawl dataset with [Apache Spark](https://spark.apache.org/) and Python:

+ [count HTML tags](./html_tag_count.py) in Common Crawl's raw response data (WARC files)

+ [count web server names](./server_count.py) in Common Crawl's metadata (WAT files or WARC files)

+ list host names and corresponding [IP addresses](./server_ip_address.py) (WAT files or WARC files)

+ [word count](./word_count.py) (term and document frequency) in Common Crawl's extracted text (WET files)

+ [extract links](./wat_extract_links.py) from WAT files and [construct the (host-level) web graph](./hostlinks_to_graph.py) – for further details about the web graphs see the project [cc-webgraph](https://github.com/commoncrawl/cc-webgraph)

+ work with the [columnar URL index](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/) (see also [cc-index-table](https://github.com/commoncrawl/cc-index-table) and notes about [schema merging](#columnar-index-and-schema-merging)):

  - run a SQL query and [export the result as a table](./cc_index_export.py)

  - select WARC records by a SQL query, parse the HTML, extract the text and [count words](./cc_index_word_count.py). Alternatively, the first step (query the columnar index) can be executed using Amazon Athena. The list of WARC record coordinates (CSV or a table created by a CTAS statement) is then passed via `--csv` or `--input_table_format`) to the Spark job.

Further information about the examples and available options is shown via the [command-line option](#command-line-options) `--help`.

## Implementing a Custom Extractor

Extending the [CCSparkJob](./sparkcc.py) isn't difficult and for many use cases it's sufficient to override a single method (`process_record`). Have a look at one of the examples, eg. to [count HTML tags](./html_tag_count.py).

## Setup

To develop and test locally, you will need to install
* Spark, see the [detailed instructions](https://spark.apache.org/docs/latest/), and
* all required Python modules by running
```
pip install -r requirements.txt
```
* (optionally, and only if you want to query the columnar index) [install S3 support libraries](#installation-of-s3-support-libraries) so that Spark can load the columnar index from S3


## Compatibility and Requirements

Tested with Spark 2.1.0 – 2.4.6 in combination with Python 2.7 or 3.5, 3.6, 3.7, and with Spark 3.0.0 - 3.2.1 in combination with Python 3.7, 3.8 and 3.9.


## Get Sample Data

To develop locally, you'll need at least three data files – one for each format used in at least one of the examples. They can be fetched from the following links:
* [warc.gz](https://data.commoncrawl.org/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/warc/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.gz)
* [wat.gz](https://data.commoncrawl.org/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/wat/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.wat.gz)
* [wet.gz](https://data.commoncrawl.org/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/wet/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.wet.gz)

Alternatively, running `get-data.sh` downloads the sample data. It also writes input files containing
* sample input as `file://` URLs
* all input of one monthly crawl as relative paths
  - to use with `--input_base_url s3://commoncrawl/` resp. `--input_base_url https://data.commoncrawl.org/`

Note that the sample data is from an older crawl (`CC-MAIN-2017-13` run in March 2017). If you want to use more recent data, please visit the [Common Crawl site](https://commoncrawl.org/the-data/get-started/).


## Process Common Crawl Data on Spark

### Running locally

First, point the environment variable `SPARK_HOME` to your Spark installation. 
Then submit a job via

```
$SPARK_HOME/bin/spark-submit ./server_count.py \
	--num_output_partitions 1 --log_level WARN \
	./input/test_warc.txt servernames
```

This will count web server names sent in HTTP response headers for the sample WARC input and store the resulting counts in the SparkSQL table "servernames" in your warehouse location defined by `spark.sql.warehouse.dir` (usually in your working directory as `./spark-warehouse/servernames`).

The output table can be accessed via SparkSQL, e.g.,

```
$SPARK_HOME/bin/pyspark
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
* [running the Spark shell and submitting Spark jobs](https://spark.apache.org/docs/latest/#running-the-examples-and-shell)
* [PySpark SQL API](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html)


### Running in Spark cluster over large amounts of data

As the Common Crawl dataset lives in the Amazon Public Datasets program, you can access and process it on Amazon AWS (in the us-east-1 [AWS region](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-regions)) without incurring any transfer costs. The only cost that you incur is the cost of the machines running your Spark cluster.

1. spinning up the Spark cluster: [AWS EMR](https://aws.amazon.com/documentation/emr/) contains a ready-to-use Spark installation but you'll find multiple descriptions on the web how to deploy Spark on a cheap cluster of AWS spot instances. See also [launching Spark on a cluster](https://spark.apache.org/docs/latest/#launching-on-a-cluster).

2. choose appropriate cluster-specific settings when [submitting jobs](https://spark.apache.org/docs/latest/submitting-applications.html) and also check for relevant [command-line options](#command-line-options) (e.g., `--num_input_partitions` or `--num_output_partitions`, see below)

3. don't forget to deploy all dependencies in the cluster, see [advanced dependency management](https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management)

4. also the the file `sparkcc.py` needs to be deployed or added as argument `--py-files sparkcc.py` to `spark-submit`. Note: some of the examples require further Python files as dependencies.


### Command-line options

All examples show the available command-line options if called with the parameter `--help` or `-h`, e.g.
```
$SPARK_HOME/bin/spark-submit ./server_count.py --help
```

#### Overwriting Spark configuration properties

There are many [Spark configuration properties](https://spark.apache.org/docs/latest/configuration.html) which allow to tune the job execution or output, see for example see [tuning Spark](https://spark.apache.org/docs/latest/tuning.html) or [EMR Spark memory tuning](https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/).

It's possible to overwrite Spark properties when [submitting the job](https://spark.apache.org/docs/latest/submitting-applications.html):

```
$SPARK_HOME/bin/spark-submit \
    --conf spark.sql.warehouse.dir=myWareHouseDir \
    ... (other Spark options, flags, config properties) \
    ./server_count.py \
    ... (program-specific options)
```

#### Authenticated S3 Access or Access Via HTTP

Since April 2022 there are two ways to access of Common Crawl data:
- using HTTP/HTTPS and the base URL `https://data.commoncrawl.org/` or `https://ds5q9oxwqwsfj.cloudfront.net/`
- using the S3 API to read the bucket `s3://commoncrawl/` requires authentication and makes an Amazon Web Services account mandatory.

This project cc-pyspark uses [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) to access WARC, WAT or WET files on `s3://commoncrawl/`. The best way is to ensure that a S3 read-only IAM policy is attached to the the IAM role of the EC2 instances where Common Crawl data is processed, see the [IAM user guide](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html). If this is no option (or if the processing is not running on AWS), there are various [options to configure credentials in boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials).


### Installation of S3 Support Libraries

While WARC/WAT/WET files are read using boto3, accessing the [columnar URL index](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/) (see option `--query` of CCIndexSparkJob) is done directly by the SparkSQL engine and requires that S3 support libraries are available. These libs are usually provided when the Spark job is run on a Hadoop cluster running on AWS (eg. EMR). However, they may not be provided for any Spark distribution and are usually absent when running Spark locally (not in a Hadoop cluster). In these situations, the easiest way is to add the libs as required packages by adding `--packages org.apache.hadoop:hadoop-aws:3.2.1` to the arguments of `spark-submit`. This will make [Spark manage the dependencies](https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management) - the hadoop-aws package and transitive dependencies are downloaded as Maven dependencies. Note that the required version of hadoop-aws package depends on the Hadoop version bundled with your Spark installation, e.g., Spark 3.2.1 bundled with Hadoop 3.2 ([spark-3.2.1-bin-hadoop3.2.tgz](https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz)).

Please also note that:
- the schema of the URL referencing the columnar index depends on the actual S3 file system implementation: it's `s3://` on EMR but `s3a://` when using [s3a](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Introducing_the_Hadoop_S3A_client.).
- (since April 2022) only authenticated S3 access is possible. This requires that access to S3 is properly set up. For configuration details, see
  [Authorizing access to EMRFS data in Amazon S3](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-plan-credentialsprovider.html)
  or [Hadoop-AWS: Authenticating with S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3).

Example call to count words in 10 WARC records host under the `.is` top-level domain:
```
$SPARK_HOME/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.2 \
    ./cc_index_word_count.py \
    --input_base_url s3://commoncrawl/ \
    --query "SELECT url, warc_filename, warc_record_offset, warc_record_length, content_charset FROM ccindex WHERE crawl = 'CC-MAIN-2020-24' AND subset = 'warc' AND url_host_tld = 'is' LIMIT 10" \
    s3a://commoncrawl/cc-index/table/cc-main/warc/ \
    myccindexwordcountoutput \
    --num_output_partitions 1 \
    --output_format json
```

### Columnar index and schema merging

The schema of the [columnar URL index](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/) has been extended over time by adding new columns. If you want to query one of the new columns (e.g., `content_languages`), the following [Spark configuration option](#overwriting-spark-configuration-properties) needs to be set:
```
--conf spark.sql.parquet.mergeSchema=true
```

However, this option impacts the query performance, so use with care! Please also read [cc-index-table](https://github.com/commoncrawl/cc-index-table) about configuration options to improve the performance of Spark SQL queries.

Alternatively, it's possible configure the table schema explicitly:
- download the [latest table schema as JSON](https://github.com/commoncrawl/cc-index-table/blob/master/src/main/resources/schema/cc-index-schema-flat.json)
- and use it by adding the command-line argument `--table_schema cc-index-schema-flat.json`.


## Credits

Examples are originally ported from Stephen Merity's [cc-mrjob](https://github.com/commoncrawl/cc-mrjob/) with the following changes and upgrades:
* based on Apache Spark (instead of [mrjob](https://mrjob.readthedocs.io/))
* [boto3](https://boto3.readthedocs.io/) supporting multi-part download of data from S3
* [warcio](https://github.com/webrecorder/warcio) a Python 2 and Python 3 compatible module to access WARC files

Further inspirations are taken from
* [cosr-back](https://github.com/commonsearch/cosr-back) written by Sylvain Zimmer for [Common Search](https://web.archive.org/web/20171117073653/https://about.commonsearch.org/). You definitely should have a look at it if you need a more sophisticated WARC processor (including a HTML parser for example).
* Mark Litwintschik's blog post [Analysing Petabytes of Websites](https://tech.marksblogg.com/petabytes-of-website-data-spark-emr.html)


## License

MIT License, as per [LICENSE](./LICENSE)
