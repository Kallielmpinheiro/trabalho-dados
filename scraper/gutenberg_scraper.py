import requests
from bs4 import BeautifulSoup
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

from common.helper import safe
from common.functions import (
    clean_summary,
    clean_text_tag,
    convert_date,
    extract_file_links,
    extract_subjects,
    extracting_reading_level,
    format_author
)


class GutenbergScraper:
    def __init__(self, db_path="data/gutenberg.db"):
        self.url = "https://www.gutenberg.org"
        self.data = []
        self.db_path = db_path

        self.session = requests.Session()
        self.author_cache = {}

        self._create_table()

    def _create_table(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT UNIQUE,
            subtitle TEXT,
            quantity_downloads INT,
            reading_level FLOAT,
            summary TEXT,
            language TEXT,
            category TEXT,
            release_date DATE,
            date_modified DATE
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS authors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            total_books INTEGER
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS book_authors (
            book_id INTEGER,
            author_id INTEGER,
            PRIMARY KEY (book_id, author_id),
            FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
            FOREIGN KEY (author_id) REFERENCES authors(id) ON DELETE CASCADE
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS subjects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS book_subjects (
            book_id INTEGER,
            subject_id INTEGER,
            PRIMARY KEY (book_id, subject_id),
            FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
            FOREIGN KEY (subject_id) REFERENCES subjects(id) ON DELETE CASCADE
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            book_id INTEGER,
            link TEXT,
            FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
        )
        """)

        conn.commit()
        conn.close()

    def _request(self, url):
        try:
            return self.session.get(url, timeout=10)
        except:
            return None

    def extract(self, items=100):
        start_index = 1
        collected = 0

        while collected < items:
            url = f"{self.url}/ebooks/search/?sort_order=downloads&start_index={start_index}"
            response = self._request(url)

            if not response:
                start_index += 25
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            books = soup.select("li.booklink")

            if not books:
                break

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(self._process_book, book) for book in books]

                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        self.data.append(result)
                        collected += 1

                        if collected >= items:
                            break

            start_index += 25

    def _process_book(self, book):
        try:
            title = clean_text_tag(book.select_one("span.title"))
            subtitle = clean_text_tag(book.select_one("span.subtitle"))
            quantity_downloads = clean_text_tag(book.select_one("span.extra"))

            link_tag = book.select_one("a.link")
            link_book = link_tag["href"]

            response = self._request(f"{self.url}{link_book}")
            if not response:
                return None

            (
                subjects,
                links,
                summary,
                language,
                category,
                release_date,
                date_modified,
                reading_level,
                authors_dict,
            ) = self._handle_book(response)

            return {
                "title": title,
                "subtitle": subtitle,
                "quantity_downloads": quantity_downloads,
                "subjects": subjects,
                "links": links,
                "summary": summary,
                "language": language,
                "category": category,
                "release_date": release_date,
                "date_modified": date_modified,
                "reading_level": reading_level,
                "authors": authors_dict
            }

        except:
            return None

    def _handle_book(self, response):
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.select_one("table.bibrec")

        subjects = safe(lambda: extract_subjects(soup), [])
        links = safe(lambda: extract_file_links(soup), [])
        summary = safe(lambda: clean_summary(soup), "")
        language = safe(lambda: clean_text_tag(table.select_one('tr[property="dcterms:language"] td')), "")
        category = safe(lambda: clean_text_tag(table.select_one('td[property="dcterms:type"]')), "")
        release_date = safe(lambda: convert_date(clean_text_tag(table.select_one('td[itemprop="datePublished"]'))), None)
        date_modified = safe(lambda: convert_date(clean_text_tag(table.select_one('td[itemprop="dateModified"]'))), None)
        reading_level = safe(lambda: extracting_reading_level(soup), None)

        authors_dict = self._handle_author(table)

        return (
            subjects,
            links,
            summary,
            language,
            category,
            release_date,
            date_modified,
            reading_level,
            authors_dict
        )

    def _handle_author(self, table):
        authors = table.select('a[itemprop="creator"]')
        result = {}

        for author in authors:
            name = format_author(clean_text_tag(author))

            if name in self.author_cache:
                result[name] = self.author_cache[name]
                continue

            start_index = 1
            total = 0

            while True:
                link = author["href"]
                response = self._request(f"{self.url}{link}?start_index={start_index}")
                if not response:
                    break

                soup = BeautifulSoup(response.text, "html.parser")
                books = soup.select("li.booklink")

                if not books:
                    break

                total += len(books)
                start_index += 25

            self.author_cache[name] = total
            result[name] = total

        return result

    def upload_books(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        cursor = conn.cursor()
        
        for book in self.data:
            cursor.execute("""
                INSERT OR IGNORE INTO books
                (title, subtitle, quantity_downloads, summary, language, category, release_date, date_modified, reading_level)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                book["title"],
                book["subtitle"],
                book["quantity_downloads"],
                book["summary"],
                book["language"],
                book["category"],
                book["release_date"],
                book["date_modified"],
                book["reading_level"],
            ))

            cursor.execute("SELECT id FROM books WHERE title = ?", (book["title"],))
            book_id = cursor.fetchone()[0]

            for name, total in book["authors"].items():
                cursor.execute(
                    "INSERT OR IGNORE INTO authors (name, total_books) VALUES (?, ?)",
                    (name, total)
                )

                cursor.execute("SELECT id FROM authors WHERE name = ?", (name,))
                author_id = cursor.fetchone()[0]

                cursor.execute(
                    "INSERT OR IGNORE INTO book_authors (book_id, author_id) VALUES (?, ?)",
                    (book_id, author_id)
                )

            for subject in book["subjects"]:
                cursor.execute("INSERT OR IGNORE INTO subjects (name) VALUES (?)", (subject,))
                cursor.execute("SELECT id FROM subjects WHERE name = ?", (subject,))
                subject_id = cursor.fetchone()[0]

                cursor.execute(
                    "INSERT OR IGNORE INTO book_subjects (book_id, subject_id) VALUES (?, ?)",
                    (book_id, subject_id)
                )

            for link in book["links"]:
                cursor.execute(
                    "INSERT INTO links (book_id, link) VALUES (?, ?)", (book_id, link)
                )

        conn.commit()
        conn.close()