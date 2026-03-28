from scraper.gutenberg_scraper import GutenbergScraper


def main():
    scraper = GutenbergScraper()
    scraper.extract()
    scraper.upload_books()


if __name__ == "__main__":
    main()
