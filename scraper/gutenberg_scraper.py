import requests
import time
from bs4 import BeautifulSoup
from common.helper import safe
import os
import sqlite3

from common.functions import (
    clean_summary,
    clean_text_tag,
    convert_date,
    extract_file_links,
    extract_subjects,
    extracting_reading_level,
)


class GutenbergScraper:
    def __init__(self, db_path="data/gutenberg.db"):
        self.url = "https://www.gutenberg.org"
        self.data = []
        self.db_path = db_path
        self._create_table()

    def _create_table(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            subtitle TEXT,
            quantity_downloads INT,
            reading_level FLOAT,
            author TEXT,
            summary TEXT,
            language TEXT,
            category TEXT,
            release_date DATE,
            date_modified DATE
        )
        """
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS subjects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
        """
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS book_subjects (
            book_id INTEGER,
            subject_id INTEGER,
            PRIMARY KEY (book_id, subject_id),
            FOREIGN KEY (book_id) REFERENCES books(id),
            FOREIGN KEY (subject_id) REFERENCES subjects(id)
        )
        """
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            book_id INTEGER,
            link TEXT,
            FOREIGN KEY (book_id) REFERENCES books(id)
        )
        """
        )

        conn.commit()
        conn.close()

    def _request(self, url, retries=3, delay=2):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
        }

        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                return response
            except:
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    return None

    def extract(self, items=10):
        start_index = 1
        collected = 0

        while collected < items:
            url = f"{self.url}/ebooks/search/?sort_order=downloads&start_index={start_index}"

            response = self._request(url)
            if response is None:
                start_index += 25
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            books = soup.select("li.booklink")
            if not books:
                break  # Nesse caso já acabaram os livros então pode parar a aplicação

            for book in books:
                if collected >= items:
                    break

                title = clean_text_tag(book.select_one("span.title"))
                subtitle = clean_text_tag(book.select_one("span.subtitle"))
                quantity_downloads = clean_text_tag(book.select_one("span.extra"))
                author = clean_text_tag(book.select_one("span.subtitle"))

                link_tag = book.select_one("a.link")
                link_book = link_tag["href"]

                book_response = self._request(f"{self.url}{link_book}")
                if book_response is None:
                    continue

                [
                    subjects,
                    links,
                    summary,
                    language,
                    category,
                    release_date,
                    date_modified,
                    reading_level,
                ] = self._handle_book(book_response)

                data = {
                    "title": title,
                    "subtitle": subtitle,
                    "quantity_downloads": quantity_downloads,
                    "author": author,
                    "subjects": subjects,
                    "links": links,
                    "summary": summary,
                    "language": language,
                    "category": category,
                    "release_date": release_date,
                    "date_modified": date_modified,
                    "reading_level": reading_level,
                }
                self.data.append(data)

                collected += 1

            start_index += 25
            time.sleep(1)

    def _handle_book(self, response):
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.select_one("table.bibrec")

        subjects = safe(lambda: extract_subjects(soup), [])
        links = safe(lambda: extract_file_links(soup), [])
        summary = safe(lambda: clean_summary(soup), "")
        language = safe(
            lambda: clean_text_tag(
                table.select_one('tr[property="dcterms:language"] td')
            ),
            "",
        )
        category = safe(
            lambda: clean_text_tag(table.select_one('td[property="dcterms:type"]')), ""
        )
        release_date = safe(
            lambda: convert_date(
                clean_text_tag(table.select_one('td[itemprop="datePublished"]'))
            ),
            None,
        )
        date_modified = safe(
            lambda: convert_date(
                clean_text_tag(table.select_one('td[itemprop="dateModified"]'))
            ),
            None,
        )
        reading_level = safe(lambda: extracting_reading_level(soup), None)

        return [
            subjects,
            links,
            summary,
            language,
            category,
            release_date,
            date_modified,
            reading_level,
        ]

    def upload_books(self):
        if not self.data:
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for book in self.data:
            cursor.execute(
                """
                INSERT INTO books (title, subtitle, quantity_downloads, author, summary, language, category, release_date, date_modified, reading_level)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    book["title"],
                    book["subtitle"],
                    book["quantity_downloads"],
                    book["author"],
                    book["summary"],
                    book["language"],
                    book["category"],
                    book["release_date"],
                    book["date_modified"],
                    book["reading_level"],
                ),
            )
            book_id = cursor.lastrowid

            for subject in book["subjects"]:
                cursor.execute(
                    "INSERT OR IGNORE INTO subjects (name) VALUES (?)", (subject,)
                )
                cursor.execute("SELECT id FROM subjects WHERE name = ?", (subject,))
                subject_id = cursor.fetchone()[0]

                cursor.execute(
                    "INSERT OR IGNORE INTO book_subjects (book_id, subject_id) VALUES (?, ?)",
                    (book_id, subject_id),
                )

            for link in book["links"]:
                cursor.execute(
                    "INSERT INTO links (book_id, link) VALUES (?, ?)", (book_id, link)
                )

        conn.commit()
        conn.close()
