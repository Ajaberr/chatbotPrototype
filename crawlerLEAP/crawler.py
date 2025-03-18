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
# Import custom URL conversion script
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
# Normalize URL to avoid duplicates with fragments/trailing slashes
################################################################################
def normalize_url(url: str) -> str:
    """
    Normalizes URLs to prevent duplicates by:
    - Removing fragments (#)
    - Ensuring consistent trailing slashes
    - Converting to lowercase
    """
    parsed = urlparse(url)
    # Remove fragments
    normalized = parsed._replace(fragment='')
    
    # Normalize path for trailing slashes
    path = parsed.path
    if not path:
        path = '/'
    elif path != '/' and path.endswith('/'):
        path = path.rstrip('/')
        
    normalized = normalized._replace(path=path)
    return normalized.geturl().lower()

################################################################################
# Write text to file, overwriting if the same URL
################################################################################
def write_text_file(filename: str, text: str) -> str:
    """
    Writes 'text' to 'filename', overwriting if it exists.
    Returns the file path written.
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text)
    return filename

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
def crawl_site_selenium(driver, start_url, max_depth, max_pages, output_dir="crawled_pages"):
    """
    Recursively crawls from start_url up to max_depth link-hops,
    limiting to max_pages in total, using Selenium to render pages.
    Returns a dict {url: cleaned_text}.
    This function also handles PDF and other file types.
    Stays on the EXACT same domain as start_url (no subdomains).
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Dictionary to track visited URLs and their content
    visited = {}
    # Queue of (url, depth) pairs to visit
    to_visit = [(start_url, 0)]
    # Set to track normalized URLs to avoid duplicates
    normalized_urls = set()
    
    # Add the normalized start URL to our tracking set
    normalized_urls.add(normalize_url(start_url))
    
    # Extract the domain to enforce same-domain crawling
    start_domain = urlparse(start_url).netloc
    
    while to_visit and len(visited) < max_pages:
        current_url, current_depth = to_visit.pop(0)
        
        # Skip if we've reached max depth
        if current_depth > max_depth:
            continue
        
        print(f"Processing URL: {current_url} (Depth: {current_depth})")
        
        # Figure out if the URL has an allowed extension
        ext = get_allowed_extension_from_url(current_url)
        
        # If it's a PDF link, handle separately
        if ext == "pdf":
            pdf_text = scrape_pdf(current_url)
            # Derive S3-friendly filename
            s3_filename = os.path.join(output_dir, urlconversion.encode_url_to_filename(current_url, "pdf"))
            written_path = write_text_file(s3_filename, pdf_text)
            print(f"PDF saved to: {written_path}")
            visited[current_url] = pdf_text
            continue
        
        # Attempt to load HTML (or other text-based pages) with Selenium
        try:
            driver.get(current_url)
            time.sleep(1)  # wait for JavaScript to load
        except Exception as e:
            error_msg = f"Error loading {current_url} with Selenium: {e}"
            print(error_msg)
            visited[current_url] = error_msg
            continue
        
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        
        # Remove script and style elements
        for script_or_style in soup(["script", "style"]):
            script_or_style.extract()
        
        # Extract visible text
        text = soup.get_text(separator="\n")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        cleaned_text = "\n".join(lines)
        
        if not cleaned_text.strip():
            cleaned_text = "Warning: No visible text extracted from this HTML page."
        
        print(f"Scraped (Selenium): {current_url} ({len(cleaned_text)} chars)")
        
        # Determine output filename based on extension
        if ext and ext != "pdf":
            s3_filename = os.path.join(output_dir, urlconversion.encode_url_to_filename(current_url, ext))
        else:
            s3_filename = os.path.join(output_dir, urlconversion.encode_url_to_filename(current_url))
        
        # Write content to file
        written_path = write_text_file(s3_filename, cleaned_text)
        print(f"Content saved to: {written_path}")
        
        # Record that we've visited this URL
        visited[current_url] = cleaned_text
        
        # Find all links on the page
        links = soup.find_all("a", href=True)
        for link in links:
            href = link["href"]
            next_url = urljoin(current_url, href)
            
            # Normalize the URL to prevent duplicates
            normalized_next_url = normalize_url(next_url)
            
            # Check if we should visit this URL:
            # 1. It's on the same domain
            # 2. We haven't visited it before
            # 3. We haven't reached max pages
            if (urlparse(next_url).netloc == start_domain and 
                normalized_next_url not in normalized_urls and 
                len(visited) < max_pages):
                
                # Add to our queue and tracking set
                to_visit.append((next_url, current_depth + 1))
                normalized_urls.add(normalized_next_url)
    
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
    chrome_options.add_argument("--window-size=1920,1080")  # Set window size for better rendering
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
    """
    driver = setup_driver(headless=True)
    
    # Set the output directory
    output_dir = "crawled_data"
    
    seed_urls = [
        "https://leap-stc.github.io",
        "https://leap.columbia.edu",
        "https://catalog.leap.columbia.edu",
    ]
    
    max_depth = 100000000000000
    max_pages = 100000000000000
    all_scraped = {}
    
    try:
        for url in seed_urls:
            print(f"\n{'='*80}\nCrawling seed URL: {url}\n{'='*80}")
            crawled_data = crawl_site_selenium(
                driver, start_url=url, max_depth=max_depth, max_pages=max_pages, output_dir=output_dir
            )
            all_scraped.update(crawled_data)
    finally:
        # Ensure driver is closed even if there's an exception
        driver.quit()
    
    # Convert the scraped data dict to a list of objects for Weaviate
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
    
    # Save the crawl results as JSON
    output_filename = os.path.join(output_dir, "crawl_results.json")
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(weaviate_objects, f, ensure_ascii=False, indent=2)
    
    print(f"\nSaved crawl results to '{output_filename}'.")
    print(f"Total pages crawled: {len(all_scraped)}")
    print("Done.")

if __name__ == "__main__":
    main()