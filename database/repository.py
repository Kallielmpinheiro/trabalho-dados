import sqlite3
import os


class BookRepository:

    def __init__(self, db_path="data/gutenberg.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._create_tables()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _create_tables(self):
        conn = self._connect()
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

    def save_books(self, books):
        conn = self._connect()
        cursor = conn.cursor()
        for book in books:
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