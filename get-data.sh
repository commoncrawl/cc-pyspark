#!/bin/bash

CRAWL=CC-MAIN-2017-13

BASE_URL=https://commoncrawl.s3.amazonaws.com

set -e

test -d input || mkdir input

if [ -e input/test.txt ]; then
	echo "File input/test.txt already exist"
	echo "... delete it to write a new one"
	exit 1
fi

for data_type in warc wat wet; do

	echo "Downloading sample $data_type file..."

	mkdir -p crawl-data/$CRAWL/
	listing=crawl-data/$CRAWL/$data_type.paths.gz
	cd crawl-data/$CRAWL/
	wget --timestamping $BASE_URL/$listing
	cd -

	file=$(gzip -dc $listing | head -1)
	mkdir -p $(dirname $file)
	cd $(dirname $file)
	wget --timestamping $BASE_URL/$file
	cd -

	echo file:$PWD/$file >>input/test_${data_type}.txt
	gzip -dc $listing | sed 's@^@s3://commoncrawl/@' \
		>input/all_${data_type}_$CRAWL.txt

done

