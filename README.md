![Common Crawl Logo](https://cdn.prod.website-files.com/6479b8d98bf5dcb4a69c4f31/649b5869af56f6df617cfb1f_CC_Logo_Blue_Auto.svg)

# Common Crawl PySpark Examples

This project provides examples how to process the Common Crawl dataset with [Apache Spark](https://spark.apache.org/) and Python:

+ [count HTML tags](./html_tag_count.py) in Common Crawl's raw response data (WARC files)

+ [count web server names](./server_count.py) in Common Crawl's metadata (WAT files or WARC files)

+ list host names and corresponding [IP addresses](./server_ip_address.py) (WAT files or WARC files)

+ [word count](./word_count.py) (term and document frequency) in Common Crawl's extracted text (WET files)

+ [extract links](./wat_extract_links.py) from WAT files and [construct the (host-level) web graph](./hostlinks_to_graph.py) – for further details about the web graphs see the project [cc-webgraph](https://github.com/commoncrawl/cc-webgraph)

+ work with the [columnar URL index](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/) (see also [cc-index-table](https://github.com/commoncrawl/cc-index-table) and the notes about [querying the columnar index](#querying-the-columnar-index)):

  - run a SQL query and [export the result as a table](./cc_index_export.py)

  - select WARC records by a SQL query, parse the HTML, extract the text and [count words](./cc_index_word_count.py). Alternatively, the first step (query the columnar index) can be executed using Amazon Athena. The list of WARC record coordinates (CSV or a table created by a CTAS statement) is then passed via `--csv` or `--input_table_format`) to the Spark job.

Further information about the examples and available options is shown via the [command-line option](#command-line-options) `--help`.

## Implementing a Custom Extractor

Extending the [CCSparkJob](./sparkcc.py) isn't difficult and for many use cases it is sufficient to override a single method (`process_record`). Have a look at one of the examples, e.g. to [count HTML tags](./html_tag_count.py).

## Setup

To develop and test locally, you'll need **Python>=3.9** and **Spark**. 

### JRE

Spark requires a 64-bit Java JRE (v8, 11, or 17 for Spark 3.5.7). Install this first. If you have an Apple Silicon device, Azul Zulu JRE is recommended for native architecture support. Ensure that either `java` is on your `$PATH` or the `$JAVA_HOME` env var points to your JRE.

### Python dependencies

Assuming you have Python already setup and a venv activated, install the `cc-pyspark` dependencies:

```
pip install -r requirements.txt
```

#### If you want to query the columnar index:
In addition to the above, [install S3 support libraries](#installation-of-s3-support-libraries) so that Spark can load the columnar index from S3.

### Spark

There are two ways to obtain Spark:
* manual installation / preinstallation
* as a pip package with `pip install`

#### For simple development or to get started quickly, the `pip install` route is recommended:

```bash
pip install pyspark==3.5.7
``` 

This will install v3.5.7 of [the PySpark python package](https://spark.apache.org/docs/latest/api/python/getting_started/index.html), which includes a local/client-only version of Spark and also adds `spark-submit` and `pyspark` to your `$PATH`. 

> If you need to interact with a remote Spark cluster, use a version of PySpark that matches the cluster version.

#### If Spark is already installed or if you want full tooling to configure a local Spark cluster:

Install Spark if (see the [Spark documentation](https://spark.apache.org/docs/latest/) for guidance). Then, ensure that `spark-submit` and `pyspark` are on your `$PATH`, or prepend `$SPARK_HOME/bin` when running eg `$SPARK_HOME/bin/spark-submit`.

> Note: The PySpark package is required if you want to run the tests in `test/`. 

## Compatibility and Requirements

Tested with Spark 3.2.3, 3.3.2, 3.4.1, 3.5.5 in combination with Python 3.8, 3.9, 3.10, 3.12 and 3.13. See the branch [python-2.7](/commoncrawl/cc-pyspark/tree/python-2.7) if you want to run the job on Python 2.7 and older Spark versions.


## Get Sample Data

To develop locally, you'll need at least three data files – one for each format used in at least one of the examples. They can be fetched from the following links:
* [warc.gz](https://data.commoncrawl.org/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/warc/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.gz)
* [wat.gz](https://data.commoncrawl.org/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/wat/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.wat.gz)
* [wet.gz](https://data.commoncrawl.org/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/wet/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.wet.gz)

Alternatively, running `get-data.sh` downloads the sample data. It also writes input files containing
* sample input as `file://` URLs
* all input of one monthly crawl as paths relative to the data bucket base URL `s3://commoncrawl/` resp. `https://data.commoncrawl.org/` – see [authenticated S3 access or access via HTTP](#authenticated-s3-access-or-access-via-http) for more information.

Note that the sample data is from an older crawl (`CC-MAIN-2017-13` run in March 2017). If you want to use more recent data, please visit the [Common Crawl site](https://commoncrawl.org/the-data/get-started/).


## Process Common Crawl Data on Spark

CC-PySpark reads the list of input files from a manifest file. Typically, these are Common Crawl WARC, WAT or WET files, but it could be any other type of file, as long it is supported by the class implementing [CCSparkJob](./sparkcc.py). The files can be given as absolute URLs or as paths relative to a base URL (option `--input_base_url`). The URL cat point to a local file (`file://`), to a remote location (typically below `s3://commoncrawl/` resp. `https://data.commoncrawl.org/`). For development and testing, you'd start with local files.

### Running locally

Spark jobs can be started using `spark-submit` (see [Setup](#setup) above if you have a manual installation of Spark):

```
spark-submit ./server_count.py \
	--num_output_partitions 1 --log_level WARN \
	./input/test_warc.txt servernames
```

This will count web server names sent in HTTP response headers for the sample WARC input and store the resulting counts in the SparkSQL table "servernames" in your warehouse location defined by `spark.sql.warehouse.dir` (usually in your working directory as `./spark-warehouse/servernames`).

The output table can be accessed via SparkSQL, e.g.,

```
$ pyspark 
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

But it's also possible to configure a different output format, for example CSV or JSON; pass `--help` on the command line for more details.

See also
* [running the Spark shell and submitting Spark jobs](https://spark.apache.org/docs/latest/#running-the-examples-and-shell)
* [PySpark SQL API](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html)

#### Debugging in an IDE

If the `.py` file for the job you want to debug is runnable (i.e. if it has a `if __name__ == "__main__":` line), you can bypass `spark-submit` and run it directly as a Python script: 

```bash
python server_count.py --num_output_partitions 1 ./input/test_warc.txt servernames`
````

Spark will complain if the output directory exists - you may want to add a preprocessing step that deletes the appropriate subdirectory under `spark-warehouse` before each run, eg `rm -rf wpark-warehouse/servernames`.

> If you have manually installed Spark you'll need to ensure the pyspark package is on your PYTHONPATH: 
> ```bash
> PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python python server_count.py --num_output_partitions 1 ./input/test_warc.txt servernames
> ```

Note that the `run_job` code is still invoked by the Spark Java binary behind the scenes, which normally prevents a debugger from attaching. To debug the `run_job` internals, it's recommended to set up a unit test and debug that; see `test/test_sitemaps_from_robotstxt` for examples of single and batched job tests.


### Running in Spark cluster over large amounts of data

As the Common Crawl dataset lives in the Amazon Public Datasets program, you can access and process it on Amazon AWS (in the us-east-1 [AWS region](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-regions)) without incurring any transfer costs. The only cost that you incur is the cost of the machines running your Spark cluster.

1. spinning up the Spark cluster: [AWS EMR](https://aws.amazon.com/documentation/emr/) contains a ready-to-use Spark installation but you'll find multiple descriptions on the web how to deploy Spark on a cheap cluster of AWS spot instances. See also [launching Spark on a cluster](https://spark.apache.org/docs/latest/#launching-on-a-cluster).

2. choose appropriate cluster-specific settings when [submitting jobs](https://spark.apache.org/docs/latest/submitting-applications.html) and also check for relevant [command-line options](#command-line-options) (e.g., `--num_input_partitions` or `--num_output_partitions`, see below)

3. don't forget to deploy all dependencies in the cluster, see [advanced dependency management](https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management)

4. also the the file `sparkcc.py` needs to be deployed or added as argument `--py-files sparkcc.py` to `spark-submit`. Note: some of the examples require further Python files as dependencies.


### Command-line options

All examples show the available command-line options if called with the parameter `--help` or `-h`, e.g.
```
spark-submit ./server_count.py --help
```

#### Overwriting Spark configuration properties

There are many [Spark configuration properties](https://spark.apache.org/docs/latest/configuration.html) which allow to tune the job execution or output, see for example see [tuning Spark](https://spark.apache.org/docs/latest/tuning.html) or [EMR Spark memory tuning](https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/).

It's possible to overwrite Spark properties when [submitting the job](https://spark.apache.org/docs/latest/submitting-applications.html):

```
spark-submit \
    --conf spark.sql.warehouse.dir=myWareHouseDir \
    ... (other Spark options, flags, config properties) \
    ./server_count.py \
    ... (program-specific options)
```

#### Authenticated S3 Access or Access Via HTTP

Since April 2022 there are two ways to access of Common Crawl data:
- using HTTP/HTTPS and the base URL `https://data.commoncrawl.org/` or `https://ds5q9oxwqwsfj.cloudfront.net/`
- using the S3 API to read the bucket `s3://commoncrawl/` requires authentication and makes an Amazon Web Services account mandatory.

The S3 API is strongly recommended as the most performant access scheme, if the data is processed in the AWS cloud and in the AWS `us-east-1` region. In contrary, if reading the data from outside the AWS cloud, HTTP/HTTPS access is the preferred option.

Dependent on the chosen access scheme, the data bucket's base URL needs to be passed using the command-line option `--input_base_url`:
- `--input_base_url https://data.commoncrawl.org/` when using HTTP/HTTPS
- `--input_base_url s3://commoncrawl/` when using the S3 API.

This project uses [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) to access WARC, WAT or WET files on `s3://commoncrawl/` over the S3 API. The best way is to ensure that a S3 read-only IAM policy is attached to the the IAM role of the EC2 instances where Common Crawl data is processed, see the [IAM user guide](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html). If this is no option (or if the processing is not running on AWS), there are various other [options to configure credentials in boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials).

Please also note that [querying the columnar index requires S3 access](#supported-access-schemes-for-the-columnar-index).


### Querying the columnar index

The example tools to query the [columnar URL index](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/) may require additional configuration and setup steps.

#### Supported access schemes for the columnar index

Querying the columnar index using cc-pyspark requires authenticated S3 access. There is no support for HTTP/HTTPS access. Please see here for more information about [supported data access schemes](#authenticated-s3-access-or-access-via-http).

#### Installation of S3 Support Libraries

While WARC/WAT/WET files are read using boto3, accessing the [columnar URL index](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/) (see option `--query` of CCIndexSparkJob) is done directly by the SparkSQL engine and requires that S3 support libraries are available. These libs are usually provided when the Spark job is run on a Hadoop cluster running on AWS (eg. EMR). However, they may not be provided for any Spark distribution and are usually absent when running Spark locally (not in a Hadoop cluster). In these situations, the easiest way is to add the libs as required packages by adding `--packages org.apache.hadoop:hadoop-aws:3.2.1` to the arguments of `spark-submit`. This will make [Spark manage the dependencies](https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management) - the hadoop-aws package and transitive dependencies are downloaded as Maven dependencies. Note that the required version of hadoop-aws package depends on the Hadoop version bundled with your Spark installation, e.g., Spark 3.2.1 bundled with Hadoop 3.2 ([spark-3.2.1-bin-hadoop3.2.tgz](https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz)).

Please also note that:
- the schema of the URL referencing the columnar index depends on the actual S3 file system implementation: it's `s3://` on EMR but `s3a://` when using [s3a](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Introducing_the_Hadoop_S3A_client.).
- (since April 2022) only authenticated S3 access is possible. This requires that access to S3 is properly set up. For configuration details, see
  [Authorizing access to EMRFS data in Amazon S3](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-plan-credentialsprovider.html)
  or [Hadoop-AWS: Authenticating with S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3).

Below an example call to count words in 10 WARC records host under the `.is` top-level domain using the `--packages` option:
```
spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.2 \
    ./cc_index_word_count.py \
    --input_base_url s3://commoncrawl/ \
    --query "SELECT url, warc_filename, warc_record_offset, warc_record_length, content_charset FROM ccindex WHERE crawl = 'CC-MAIN-2020-24' AND subset = 'warc' AND url_host_tld = 'is' LIMIT 10" \
    s3a://commoncrawl/cc-index/table/cc-main/warc/ \
    myccindexwordcountoutput \
    --num_output_partitions 1 \
    --output_format json
```

#### Columnar index and schema merging

The schema of the [columnar URL index](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/) has been extended over time by adding new columns. If you want to query one of the new columns (e.g., `content_languages`), the following [Spark configuration option](#overwriting-spark-configuration-properties) needs to be set:
```
--conf spark.sql.parquet.mergeSchema=true
```

However, this option impacts the query performance, so use with care! Please also read [cc-index-table](https://github.com/commoncrawl/cc-index-table) about configuration options to improve the performance of Spark SQL queries.

Alternatively, it's possible configure the table schema explicitly:
- download the [latest table schema as JSON](https://github.com/commoncrawl/cc-index-table/blob/master/src/main/resources/schema/cc-index-schema-flat.json)
- and use it by adding the command-line argument `--table_schema cc-index-schema-flat.json`.


### Using FastWARC to parse WARC files

> [FastWARC](https://resiliparse.chatnoir.eu/en/latest/man/fastwarc.html) is a high-performance WARC parsing library for Python written in C++/Cython. The API is inspired in large parts by WARCIO, but does not aim at being a drop-in replacement.

Replacing [FastWARC](https://resiliparse.chatnoir.eu/en/latest/man/fastwarc.html) can speed up job execution by 25% if little custom computations are done and most of the time is spent for parsing WARC files.

To use FastWARC
- the job class must inherit from [CCFastWarcSparkJob](./sparkcc_fastwarc.py) instead of [CCSparkJob](./sparkcc.py). See [ServerCountFastWarcJob](./server_count_fastwarc.py) for an example.
- when running the job in a Spark cluster, `sparkcc_fastwarc.py` must be passed via `--py-files` in addition to `sparkcc.py` and further job-specific Python files. See also [running in a Spark cluster](#running-in-spark-cluster-over-large-amounts-of-data).

Some differences between the warcio and FastWARC APIs are hidden from the user in methods implemented in [CCSparkJob](./sparkcc.py) and [CCFastWarcSparkJob](./sparkcc_fastwarc.py) respectively. These methods allow to access WARC or HTTP headers and the payload stream in a unique way, regardless of whether warcio or FastWARC are used.

However, it's recommended that you carefully verify that your custom job implementation works in combination with FastWARC. There are subtle differences between the warcio and FastWARC APIs, including the underlying classes (WARC/HTTP headers and stream implementations). In addition, FastWARC does not support for legacy ARC files and does not automatically decode HTTP content and transfer encodings (see [Resiliparse HTTP Tools](https://resiliparse.chatnoir.eu/en/latest/man/parse/http.html#read-chunked-http-payloads)). While content and transfer encodings are already decoded in Common Crawl WARC files, this may not be the case for WARC files from other sources. See also [WARC 1.1 specification, http/https response records](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#http-and-https-schemes).


## Running the Tests

To run the tests in `test/` you will need to add `.` and `test` to the PYTHONPATH:

```bash
PYTHONPATH=$PYTHONPATH:.:test pytest -v test
```

or if you have a manual installation of Spark:

```bash
PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:.:test pytest -v test
```


## Credits

Examples are originally ported from Stephen Merity's [cc-mrjob](https://github.com/commoncrawl/cc-mrjob/) with the following changes and upgrades:
* based on Apache Spark (instead of [mrjob](https://mrjob.readthedocs.io/))
* [boto3](https://boto3.readthedocs.io/) supporting multi-part download of data from S3
* [warcio](https://github.com/webrecorder/warcio) a Python 2 and Python 3 compatible module for accessing WARC files

Further inspirations are taken from
* [cosr-back](https://github.com/commonsearch/cosr-back) written by Sylvain Zimmer for [Common Search](https://web.archive.org/web/20171117073653/https://about.commonsearch.org/). You should definitely take a look at it if you need a more sophisticated WARC processor (including an HTML parser for example).
* Mark Litwintschik's blog post [Analysing Petabytes of Websites](https://tech.marksblogg.com/petabytes-of-website-data-spark-emr.html)


## License

MIT License, as per [LICENSE](./LICENSE)
