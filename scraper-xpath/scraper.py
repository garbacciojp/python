// Simple scraper XPath Scraper to scrape HTML information using XPath statements.
// Run in terminal.
// Submit XPATH statement as an input, the script requests this.
// Hit return / enter and let the script work.
// Always ensure the input file and script are stored in the same folder.

import requests
from lxml import etree
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import threading

# Function to extract URLs from the CSV file
def extract_urls_from_csv(file_path):
    df = pd.read_csv(file_path)
    return df['URL'].tolist()

# Function to extract information from a single URL
def extract_info(url, xpath_expr, counter_lock, counter):
    try:
        response = requests.get(url, timeout=10)  # Added timeout for network requests
        if response.status_code == 200:
            tree = etree.HTML(response.content)
            info = tree.xpath(xpath_expr)
            with counter_lock:
                counter[0] += 1
                print(f"Processed {counter[0]}: {url}")
            if isinstance(info, list) and len(info) > 0:
                # If the result is a list, join it into a single string for consistency
                return url, ' '.join(map(str, info)).strip()
            elif isinstance(info, (str, float, int)):
                # Handle string, float, and integer results
                return url, str(info).strip()
            else:
                return url, "Information not found"
        else:
            with counter_lock:
                counter[0] += 1
                print(f"Processed {counter[0]}: {url} - Failed to retrieve page")
            return url, "Failed to retrieve page"
    except requests.RequestException as e:  # More specific exception handling for requests
        with counter_lock:
            counter[0] += 1
            print(f"Processed {counter[0]}: {url} - Error: {e}")
        return url, f"Error: {e}"
    except Exception as e:
        with counter_lock:
            counter[0] += 1
            print(f"Processed {counter[0]}: {url} - Unexpected Error: {e}")
        return url, f"Unexpected Error: {e}"

# Function to process URLs using multiple threads
def process_urls(urls, xpath_expr, max_workers=10):
    results = []
    counter_lock = threading.Lock()
    counter = [0]  # Mutable counter to track progress
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(extract_info, url, xpath_expr, counter_lock, counter): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                results.append((url, f"Error: {e}"))
    return results

# Main function
def main():
    input_csv = 'input-urls.csv'  # The path to your CSV file
    output_csv = 'output-urls.csv'  # The path to save the output CSV file
    urls = extract_urls_from_csv(input_csv)
    if not urls:
        print("No URLs found in the CSV file.")
        return

    # Get the XPath expression from the user
    xpath_expr = input("Please enter the XPath expression to extract the information: ").strip()
    if not xpath_expr:
        print("No XPath expression provided.")
        return

    print(f"Processing {len(urls)} URLs with XPath: {xpath_expr}")
    results = process_urls(urls, xpath_expr, max_workers=10)

    # Save the results to a CSV file
    df = pd.DataFrame(results, columns=['URL', 'Information'])
    df.to_csv(output_csv, index=False)
    print(f"Results saved to {output_csv}")

if __name__ == '__main__':
    main()
