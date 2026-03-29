import re
from datetime import datetime


def clean_summary(soup):
    readmore_container = soup.select_one("span.readmore-container").text.strip()
    if not readmore_container:
        return ""

    summary = re.sub(r"\.\.\.\s*Read More", "", readmore_container)
    summary = re.sub(r"Show Less", "", summary)
    summary = " ".join(summary.split())
    return summary


def clean_text_tag(tag):
    return tag.text.strip()


def convert_date(date):
    return datetime.strptime(date, "%b %d, %Y")


def extract_file_links(soup):
    table = soup.select_one("table.files")
    links = []

    if table:
        for a_tag in table.select('td[property="dcterms:format"] a'):
            href = a_tag.get("href")
            if href:
                full_url = f"https://www.gutenberg.org{href}"
                links.append(full_url)

    return links


def extract_subjects(soup):
    subjects = []

    subjects_tag = soup.select('td[property="dcterms:subject"]')
    for subject_tag in subjects_tag:
        subject = subject_tag.select_one("a.block").text.strip()
        subjects.append(subject)

    return subjects


def extracting_reading_level(soup):
    th = soup.find("th", string="Reading Level")
    td = th.find_next_sibling("td")
    reading_text = td.get_text(strip=True)

    match = re.search(r"Reading ease score:\s*([\d.]+)", reading_text)
    return float(match.group(1))
