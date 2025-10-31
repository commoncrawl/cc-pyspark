from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse import detect_encoding
from resiliparse.parse.html import HTMLTree


class HTMLParser(object):
    """
    HTML parser using Resiliparse
    """

    def html_to_text(self, tree, **kwargs) -> str:
        """
        Convert HTML content to plain text using Resiliparse.

        Returns:
            str: Extracted plain text with scripts and styles removed
        """
        text = extract_plain_text(tree, **kwargs)
        return text

    def get_html_tree(self, page: bytes, encoding: str=None, **kwargs) -> HTMLTree:
        """
        Get the HTML tree object

        Args:
            page (bytes): Raw HTML content as bytes
            encoding (str, optional): Specific character encoding to use. If None, auto-detection is attempted
            **kwargs: Additional arguments passed to extract_plain_text:
                Refer here https://resiliparse.chatnoir.eu/en/latest/api/extract/html2text.html#resiliparse.extract.html2text.extract_plain_text for accepted arguments.
        Returns:
            str: Extracted plain text content
        """
        if not encoding:
            encoding = detect_encoding(page)
        tree = HTMLTree.parse_from_bytes(page, encoding, **kwargs)
        return tree