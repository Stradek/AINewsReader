import os
import glob
import time
import zipfile
import csv
import json
import requests
import random
import vertexai
from vertexai.preview.generative_models import GenerativeModel
from fake_useragent import UserAgent
from bs4 import BeautifulSoup

def initialize_config(config_file):
    config_template = {
        "project": "your-project-id",
        "location": "your-region",
        'exclude_urls_with_string': ['youtube.com'],  # Add strings to exclude here
        "model_id": "your-endpoint-id",
        'proxies': []
    }
    if not os.path.exists(config_file):
        with open(config_file, 'w') as file:
            json.dump(config_template, file, indent=4)
        print(f'Config file created at {config_file} with template: {config_template}')
        exit()
    else:
        print(f'Config file already exists at {config_file}')

def fetch_website_data(url, proxies, user_agent):
    # TODO: Add cookies key to config but need to assure secure storage

    # Random delay to simulate human behavior
    time.sleep(random.uniform(1, 3))
    
    # Create a headers dictionary with a dynamic User-Agent
    headers = {
        'User-Agent': user_agent.random,  # Random User-Agent each time
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }

    # Optional: Set up a session for better performance
    session = requests.Session()

    # Try fetching the website
    try:
        response = session.get(url, headers=headers, proxies=proxies, timeout=10)
        
        # Check if request was successful (status code 200)
        response.raise_for_status()
        
        # Ensure the response is interpreted as plain text HTML
        response.encoding = 'utf-8'
        
        return response.text

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error fetching {url}: {e}")

    except requests.exceptions.RequestException as e:
        print(f"Request Error fetching {url}: {e}")

    except Exception as e:
        print(f"Unexpected Error fetching {url}: {e}")

    print()
    return None

def extract_news_content(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style elements
    for script_or_style in soup(['script', 'style']):
        script_or_style.decompose()
    
    # Get text content
    text = soup.get_text(separator=' ')
    
    # Remove extra whitespace
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = ' '.join(chunk for chunk in chunks if chunk)
    
    return text

def process_websites_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        websites_data = json.load(file)
    
    cleaned_data = {}
    for url, html_content in websites_data.items():
        if html_content:
            news_content = extract_news_content(html_content)
            cleaned_data[url] = news_content
    
    return cleaned_data

def main():
    # Determine the project directories and files
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..'))

    resources_path = os.path.join(project_root, 'resources')
    temp_dir_path = os.path.join(project_root, 'temp')

    config_file = os.path.join(project_root, 'config.json')
    websites_data_file = os.path.join(temp_dir_path, 'websites_data.json')
    cleaned_webistes_data_file = os.path.join(temp_dir_path, 'cleaned_websites_data.json')

    # Initialize config file
    initialize_config(config_file)

    # Load configuration
    with open(config_file, 'r') as file:
        config = json.load(file)

    project_id = config['project']
    location = config['location']
    model_id = config['model_id']

    # Create the resources directory if it doesn't exist
    if not os.path.exists(resources_path):
        os.makedirs(resources_path)
        print(f'Resources directory created at {resources_path}')
        print('Place zip with news data CSV from Notion database into resources directory to run the script')
        return

    # Find the latest zip file in the resources directory
    zip_files = glob.glob(os.path.join(resources_path, '*.zip'))

    if len(zip_files) == 0:
        print('No zip file has been found in resources directory')
        print('Place zip with news data CSV from Notion database into resources directory to run the script')
        return

    latest_zip_file = max(zip_files, key=os.path.getctime)

    # Create the extraction directory if it doesn't exist
    os.makedirs(temp_dir_path, exist_ok=True)

    # Unzip the latest zip file
    with zipfile.ZipFile(latest_zip_file, 'r') as zip_ref:
        zip_ref.extractall(temp_dir_path)

    print(f'Files extracted to {temp_dir_path} from {latest_zip_file}')

    # Find the CSV file which name doesn't end with _all
    csv_files = glob.glob(os.path.join(temp_dir_path, '*.csv'))
    target_csv_file = None

    for csv_file in csv_files:
        if not csv_file.endswith('_all.csv'):
            target_csv_file = csv_file
            break

    if not target_csv_file:
        print('No target CSV file found that does not end with _all')
        return

    print(f'Found target CSV file: {target_csv_file}')

    # Read the target CSV file and add all URLs to a set, excluding those that include strings in exclude_strings
    exclude_strings = config["exclude_urls_with_string"]

    urls = set()
    with open(target_csv_file, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            url = row['URL']  # Assuming the column name is 'URL'
            if not any(exclude_string in url for exclude_string in exclude_strings):
                urls.add(url)

    print(f'URLs found: {urls}')

    # Initialize the Vertex AI client
    # vertexai.init(project=project_id, location=location)

    # Define the GenerativeModel
    # model = GenerativeModel(model_id)
    
    # response = model.generate_content("How are you doing buddy?")
    # print(response)

    # Scrap websites data 
    url_to_website_data = {}

    user_agent = UserAgent()

    for url in urls:
        data = fetch_website_data(url, config['proxies'], user_agent)
        if data:
            print(f"Data from {url}: {data[:100]}...")  # Print the first 100 characters of the data
            print()

        url_to_website_data[url] = data

    # Save the website data to a JSON file
    with open(websites_data_file, 'w', encoding='utf-8') as json_file:
        json.dump(url_to_website_data, json_file, ensure_ascii=False, indent=4)

    # Process the websites data
    cleaned_data = process_websites_data(websites_data_file)
    with open(cleaned_webistes_data_file, 'w', encoding='utf-8') as file:
        json.dump(cleaned_data, file, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    main()