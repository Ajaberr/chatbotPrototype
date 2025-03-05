import os
import time
import json
import io
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# PyMuPDF for PDF extraction
import fitz  

# 1) Import your custom URL conversion script
import urlconversion

################################################################################
# Utility: Decide if a URL has one of the allowed extensions
################################################################################
ALLOWED_EXTENSIONS = {"html", "pdf", "md", "txt", "csv", "xls", "xlsx", "doc", "docx"}

def get_allowed_extension_from_url(url: str):
    """
    Parses the URL to see if its last path segment has an allowed extension.
    Returns that extension (e.g., 'pdf') if present, or None otherwise.
    """
    parsed_url = urlparse(url)
    filename = parsed_url.path.split("/")[-1]  # e.g. 'somefile.pdf' or ''
    if "." in filename:
        ext = filename.split(".")[-1].lower()
        if ext in ALLOWED_EXTENSIONS:
            return ext
    return None


################################################################################
# Safely write text to file without overwriting
################################################################################
def safe_write_text_file(filename: str, text: str) -> str:
    """
    Writes 'text' to 'filename'. If the file already exists,
    append a numeric suffix to avoid overwriting.
    Returns the actual file path written.
    """
    base, ext = os.path.splitext(filename)
    unique_filename = filename
    counter = 1

    while os.path.exists(unique_filename):
        unique_filename = f"{base}_{counter}{ext}"
        counter += 1

    with open(unique_filename, "w", encoding="utf-8") as f:
        f.write(text)

    return unique_filename


################################################################################
# PDF scraping with PyMuPDF
################################################################################
def scrape_pdf(url: str) -> str:
    """
    Downloads and extracts text from a PDF file at 'url' using PyMuPDF (fitz).
    Returns the extracted text as a string.
    - If no text is found, returns a warning that it may be image-based.
    - If there's an error, returns an error string.
    """
    try:
        response = requests.get(url)
        if response.status_code != 200:
            return f"Error fetching PDF: {response.status_code}"

        pdf_bytes = io.BytesIO(response.content)

        # Open the PDF with fitz (PyMuPDF)
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        all_text = ""

        # Extract text from each page
        for page in doc:
            page_text = page.get_text("text")  # or "blocks"/"words" if needed
            all_text += page_text + "\n"

        doc.close()

        if not all_text.strip():
            return "Warning: PDF may be image-based or PyMuPDF couldn't extract text."

        print(f"Scraped PDF: {url} ({len(all_text)} chars)")
        return all_text

    except Exception as e:
        return f"Error processing PDF {url}: {e}"


################################################################################
# Main crawl function using Selenium (recursive), EXACT DOMAIN only
################################################################################
def crawl_site_selenium(driver, start_url, max_depth, max_pages, visited=None):
    """
    Recursively crawls from start_url up to max_depth link-hops,
    limiting to max_pages in total, using Selenium to render pages.
    Returns a dict {url: cleaned_text}.
    This function also handles PDF and other file types.
    Stays on the EXACT same domain as start_url (no subdomains).
    """
    if visited is None:
        visited = {}

    # If we already visited this URL, skip
    if start_url in visited:
        return visited

    start_domain = urlparse(start_url).netloc  # e.g. "catalog.leap.columbia.edu"

    # Figure out if the URL has an allowed extension
    ext = get_allowed_extension_from_url(start_url)

    # If it's a PDF link, handle separately
    if ext == "pdf":
        pdf_text = scrape_pdf(start_url)
        # Derive S3-friendly filename
        s3_filename = urlconversion.encode_url_to_filename(start_url, "pdf")
        written_path = safe_write_text_file(s3_filename, pdf_text)
        print(f"PDF saved to: {written_path}")

        visited[start_url] = pdf_text
        return visited

    # Attempt to load HTML (or other text-based pages) with Selenium
    try:
        driver.get(start_url)
        time.sleep(1)  # wait for JavaScript to load
    except Exception as e:
        error_msg = f"Error loading {start_url} with Selenium: {e}"
        print(error_msg)
        visited[start_url] = error_msg
        return visited

    page_source = driver.page_source
    soup = BeautifulSoup(page_source, "html.parser")

    # Remove script and style elements
    for script_or_style in soup(["script", "style"]):
        script_or_style.extract()

    # Extract visible text
    text = soup.get_text(separator="\n")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    cleaned_text = " ".join(lines)

    if not cleaned_text.strip():
        cleaned_text = "Warning: No visible text extracted from this HTML page."

    print(f"Scraped (Selenium): {start_url} ({len(cleaned_text)} chars)")

    if ext and ext != "pdf":
        s3_filename = urlconversion.encode_url_to_filename(start_url, ext)
    else:
        s3_filename = urlconversion.encode_url_to_filename(start_url)

    written_path = safe_write_text_file(s3_filename, cleaned_text)
    print(f"HTML/text saved to: {written_path}")

    visited[start_url] = cleaned_text

    # Recursively follow links if depth allows
    if max_depth > 0:
        links = soup.find_all("a", href=True)
        for link in links:
            if len(visited) >= max_pages:
                break

            href = link["href"]
            next_url = urljoin(start_url, href)

            # EXACT same domain check
            if urlparse(next_url).netloc == start_domain:
                if next_url not in visited:
                    crawl_site_selenium(driver, next_url, max_depth - 1, max_pages, visited)

    return visited


################################################################################
# Selenium driver setup
################################################################################
def setup_driver(headless=True):
    """
    Configures and returns a Selenium WebDriver (Chrome) instance
    with the desired options. Uses webdriver_manager to install
    the correct ChromeDriver version automatically.
    """
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


################################################################################
# Main runner
################################################################################
def main():
    """
    Crawls given seed URLs and saves the results in a JSON file,
    staying strictly on the same domain for each seed URL.
    Uses urlconversion.encode_url_to_filename() to store files locally
    in a manner consistent with what S3 (Bedrock) expects.
    Now uses PyMuPDF (fitz) to extract PDF text.
    """
    driver = setup_driver(headless=True)

    seed_urls = [
        "https://leap-stc.github.io",
        "https://leap.columbia.edu",
        "https://catalog.leap.columbia.edu",
    ]

    max_depth = 100
    max_pages = 10000

    all_scraped = {}
    for url in seed_urls:
        crawled_data = crawl_site_selenium(
            driver, start_url=url, max_depth=max_depth, max_pages=max_pages
        )
        all_scraped.update(crawled_data)

    driver.quit()

    # Convert the scraped data dict to a list of objects
    weaviate_objects = []
    for url, text in all_scraped.items():
        obj = {
            "class": "WebPage",
            "title": "",
            "videoId": "",
            "url": url,
            "transcript": text
        }
        weaviate_objects.append(obj)

    output_filename = "crawl_results.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(weaviate_objects, f, ensure_ascii=False, indent=2)

    print(f"\nSaved crawl results to '{output_filename}'.")
    print("Done.")


if __name__ == "__main__":
    main()
