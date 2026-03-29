from common.functions.functions import (
    clean_text_tag,
    clean_summary,
    extract_subjects,
    extract_file_links,
    convert_date,
    extracting_reading_level
)
from common.helper import safe

class BookParser:
    def parse_list_item(self, book):
        return {
            "title": clean_text_tag(book.select_one("span.title")),
            "subtitle":  clean_text_tag(book.select_one("span.subtitle")),
            "quantity_downloads": clean_text_tag(book.select_one("span.extra")),
            "link": book.select_one("a.link")["href"]
        }

    def parse_book_page(self, soup):
        table = soup.select_one("table.bibrec")

        return {
            "subjects": safe(lambda: extract_subjects(soup), []),
            "links": safe(lambda: extract_file_links(soup), []),
            "summary": safe(lambda: clean_summary(soup), ""),
            "language": safe(
                lambda: clean_text_tag(table.select_one('tr[property="dcterms:language"] td')),
                ""
            ),
            "category": safe(
                lambda: clean_text_tag(table.select_one('td[property="dcterms:type"]')),
                ""
            ),
            "release_date": safe(
                lambda: convert_date(
                    clean_text_tag(table.select_one('td[itemprop="datePublished"]'))
                ),
                None
            ),
            "date_modified": safe(
                lambda: convert_date(
                    clean_text_tag(table.select_one('td[itemprop="dateModified"]'))
                ),
                None
            ),
            "reading_level": safe(lambda: extracting_reading_level(soup), None),
        }