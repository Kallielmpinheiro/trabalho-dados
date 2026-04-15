from pathlib import Path
import sys
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.repository import BookRepository

SQL_QUERY_BOOKS = "SELECT * FROM books"
SQL_QUERY_SUMMARIES = "SELECT * FROM book_authors"
SQL_QUERY_LINKS = "SELECT * FROM links"
SQL_QUERY_SUBJECTS = "SELECT * FROM subjects"

def create_dataset():
    conn = BookRepository()._connect()
    try:
        df_books = pd.read_sql_query(SQL_QUERY_BOOKS, conn)
        df_books = df_books.drop_duplicates()                        
        df_books = df_books.dropna(subset=["summary", "reading_level"])      
        
        df_authors = pd.read_sql_query(SQL_QUERY_SUMMARIES, conn)
        df_links = pd.read_sql_query(SQL_QUERY_LINKS, conn)
        df_subjects = pd.read_sql_query(SQL_QUERY_SUBJECTS, conn)

        output_file = PROJECT_ROOT / "data" / "dataset_final.xlsx"
        
        with pd.ExcelWriter(output_file) as writer:
            df_books.to_excel(writer, sheet_name='Books', index=False)
            df_authors.to_excel(writer, sheet_name='Authors', index=False)
            df_links.to_excel(writer, sheet_name='Links', index=False)
            df_subjects.to_excel(writer, sheet_name='Subjects', index=False)
    finally:
        conn.close()

if __name__ == "__main__":
    create_dataset()