#help
import requests
import json
from io import StringIO,BytesIO
import gzip
from warcio.archiveiterator import ArchiveIterator,ArchiveLoadFailed
from bs4 import BeautifulSoup
from unidecode import unidecode
import pandas as pd



def search_domain(domain):
    record_list = []
    print ("[*] Trying target domain: %s" % domain)

    for index in index_list:
        print ("[*] Trying index %s" % index)
        cc_url  = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
        cc_url += "url=%s&matchType=domain&output=json" % domain

        response = requests.get(cc_url)

        if response.status_code == 200:
            records = response.content.splitlines()
            for record in records:
                record_list.append(json.loads(record))
            print ("[*] Added %d results." % len(records))
    print ("[*] Found a total of %d hits." % len(record_list))
    return record_list



#choosing specific article



def download_page(record):

    offset, length = int(record['offset']), int(record['length'])
    offset_end = offset + length - 1

    # We'll get the file via HTTPS so we don't need to worry about S3 credentials
    # Getting the file on S3 is equivalent however - you can request a Range
    prefix = 'https://commoncrawl.s3.amazonaws.com/'

    # We can then use the Range header to ask for just this set of bytes
    resp = requests.get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})

    # The page is stored compressed (gzip) to save space
    # We can extract it using the GZIP library
    raw_data = BytesIO(resp.content)
    f = gzip.GzipFile(fileobj=raw_data)

    # What we have now is just the WARC response, formatted:
    data = f.read()

    response = ""

    if len(data):
        try:
            response = data.strip()
        except:
            pass

    response = response.decode('UTF-8')
    soup = BeautifulSoup(response,"html.parser")
    for script in soup(["script", "style"]):
        script.extract()

    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    words = text.split()

    clean_words = []
    return words




index_list = ['2017-39']
domain = 'nytimes.com/section/technology'
nyt_urls = []
word_list = []

nyt = search_domain(domain)


def build_table(n):
    nyt_sub = nyt[10:10+n]
    for i in nyt_sub:
        nyt_urls.append(i['url'])


    for i in nyt_sub:
        vocab = download_page(i)
        word_list.append(vocab)


    s1 = pd.Series(nyt_urls,name="urls")
    s2 = pd.Series(word_list,name = "vocab_lists")
    df = pd.concat([s1,s2],axis=1)
    return df
