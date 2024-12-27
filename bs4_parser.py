from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector


class HTMLParser(object):
    """
    HTML parser using BeautifulSoup4
    """

    def html_to_text(self, html_tree: BeautifulSoup) -> str:
        """
        Convert HTML content to plain text using BeautifulSoup4.

        Returns:
            str: Extracted plain text with scripts and styles removed
        """
        for script in html_tree(['script', 'style']):
            script.extract()
        text = html_tree.get_text(' ', strip=True)
        return text

    def get_html_tree(self, page: bytes, encoding: str=None, features='lxml', **kwargs) -> BeautifulSoup:
        """
        Return the HTML tree object

        Args:
            page (bytes): Raw HTML content as bytes
            encoding (str, optional): Specific character encoding to use. If None, auto-detection is attempted
            features: Parser to be used (default='lxml'). Refer https://www.crummy.com/software/BeautifulSoup/bs4/doc/#installing-a-parser for supported parsers.
            **kwargs: Additional arguments passed to BeautifulSoup constructor.
             Refer here https://www.crummy.com/software/BeautifulSoup/bs4/doc/#bs4.BeautifulSoup for accepted arguments.

        Returns:
            BeautifulSoup: HTML tree object
        """
        if not encoding:
            for encoding in EncodingDetector(page, is_html=True).encodings:
                # take the first detected encoding
                break
        soup = BeautifulSoup(page, features, from_encoding=encoding, **kwargs)
        return soup