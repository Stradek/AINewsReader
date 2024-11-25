import os
import glob
import zipfile
import csv
import json
from google.cloud import aiplatform

def initialize_config(config_file):
    config_template = {
        "project": "your-project-id",
        "location": "your-region"
    }
    if not os.path.exists(config_file):
        with open(config_file, 'w') as file:
            json.dump(config_template, file, indent=4)
        print(f'Config file created at {config_file} with template: {config_template}')
    else:
        print(f'Config file already exists at {config_file}')

def main():
    # Determine the project root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..'))

    config_file = os.path.join(project_root, 'config.json')
    initialize_config(config_file)

    # Load configuration
    with open(config_file, 'r') as file:
        config = json.load(file)

    project_id = config['project']
    location = config['location']

    # Define the path to the resources directory and the extraction directory
    resources_path = os.path.join(project_root, 'resources')
    extraction_path = os.path.join(project_root, 'temp')

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
    os.makedirs(extraction_path, exist_ok=True)

    # Unzip the latest zip file
    with zipfile.ZipFile(latest_zip_file, 'r') as zip_ref:
        zip_ref.extractall(extraction_path)

    print(f'Files extracted to {extraction_path} from {latest_zip_file}')

    # Find the CSV file which name doesn't end with _all
    csv_files = glob.glob(os.path.join(extraction_path, '*.csv'))
    target_csv_file = None

    for csv_file in csv_files:
        if not csv_file.endswith('_all.csv'):
            target_csv_file = csv_file
            break

    if not target_csv_file:
        print('No target CSV file found that does not end with _all')
        return

    print(f'Found target CSV file: {target_csv_file}')

    # Read the target CSV file and add all URLs to a set
    urls = set()
    with open(target_csv_file, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            urls.add(row['URL'])  # Assuming the column name is 'URL'

    print(f'URLs found: {urls}')

    # Initialize the Vertex AI client
    aiplatform.init(project=project_id, location=location)

    # Define the endpoint and model ID
    endpoint = aiplatform.Endpoint(endpoint_name=f'projects/{project_id}/locations/{location}/endpoints/your-endpoint-id')

    # Make predictions using the Vertex AI model
    for url in urls:
        response = endpoint.predict(instances=[{'url': url}])
        print(f'Prediction for {url}: {response.predictions}')

if __name__ == "__main__":
    main()