from scraper.gutenberg_scraper import GutenbergScraper
from database.repository import BookRepository

def main():
    scraper = GutenbergScraper()
    repo = BookRepository()

    books = scraper.extract()  
    repo.save_books(books)              


if __name__ == "__main__":
    main()