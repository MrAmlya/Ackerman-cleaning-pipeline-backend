from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS
import data_extraction
import data_aggregation_cleaning
import final_data_cleaning
import os
import time
from datetime import datetime, timedelta
import threading

app = Flask(__name__)

# Apply CORS to allow requests from any origin
CORS(app, resources={r"/api/*": {"origins": "http://localhost:3000"}})

# Define paths for extraction and cleaning processes
PATH_EXTRACTION = "./extracted_data"
PATH_CLEANING = "./temp_clean"
FINAL_CLEANED_PATH = "./final_cleaned_files"
EXTRACTED_DATA_PATH = "./extracted_data"

# Define the time threshold in seconds (24 hours)
TIME_THRESHOLD = 24 * 60 * 60  # 24 hours in seconds

@app.route('/api/process-data', methods=['POST'])
def extract_data():
    # Parse JSON input from the request
    data = request.get_json()
    input_category = data.get("inputCategory")
    input_string = data.get("inputString")
    item_list = [item.strip() for item in input_string.split(',')]

    if input_category == "P":
        data_extraction.country_extraction(item_list)
    elif input_category == "Y":
        data_extraction.year_extraction(item_list)
    elif input_category == "C":
        data_extraction.cause_of_death(item_list)
    elif input_category == "L":
        data_extraction.last_name(item_list)

    data_aggregation_cleaning.file_reader(PATH_EXTRACTION, item_list)
    final_data_cleaning.file_reader_final_cleaning(PATH_CLEANING, item_list)

    final_file_name = f"{item_list[0]}Cleaned_Final.csv"
    final_file_path = os.path.join("./final_cleaned_files", final_file_name)

    return jsonify({
        "status": "200",
        "message": "Data processed successfully",
        "items": item_list,
        "download_url": f"/api/download/{final_file_name}"  # Provide the download link
    })


@app.route('/api/download/<file_name>', methods=['GET'])
def download_file(file_name):
    # file_path = os.path.join("./final_cleaned_files", file_name)
    file_path = os.path.abspath(os.path.join(".", "final_cleaned_files", file_name))

    # Check if the file exists
    if os.path.exists(file_path):
        try:
            return send_file(file_path, as_attachment=True)
        except Exception as e:
            return jsonify({"status": "500", "message": f"Failed to download file: {str(e)}"})
    else:
        return jsonify({"status": "404", "message": "File not found"})
    

def delete_old_files():
    while True:
        now = time.time()

        # Delete files in EXTRACTED_DATA_PATH older than TIME_THRESHOLD seconds. #
        for filename in os.listdir(FINAL_CLEANED_PATH):
            file_path = os.path.join(FINAL_CLEANED_PATH, filename)
            if os.path.isfile(file_path):
                file_creation_time = os.path.getctime(file_path)
                # Delete file if it is older than the threshold
                if now - file_creation_time > TIME_THRESHOLD:
                    os.remove(file_path)
                    print(f"Deleted old file: {filename}")

        # Check for files in EXTRACTED_DATA older than TIME_THRESHOLD seconds
        for filename in os.listdir(EXTRACTED_DATA_PATH):
            file_path = os.path.join(EXTRACTED_DATA_PATH, filename)
            if os.path.isfile(file_path):
                file_creation_time = os.path.getctime(file_path)
                # Delete file if it is older than the threshold
                if now - file_creation_time > TIME_THRESHOLD:
                    os.remove(file_path)
                    print(f"Deleted old file: {filename}")
        # Sleep before the next check
        time.sleep(60 * 60)  # Check every hour
    

if __name__ == '__main__':

     # Start the background thread to delete old files
    file_cleanup_thread = threading.Thread(target=delete_old_files, daemon=True)
    file_cleanup_thread.start()

    # Run the Flask app
    app.run(debug=True)
