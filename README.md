# BITSoM_BA_25071891-salesanalytics.system
BITSoM BA Assignment - Module 3

# Sales Data Analytics System

A comprehensive Python system for processing, analyzing, and reporting on sales data. This system handles messy sales transaction files, cleans data according to business rules, performs analysis, and integrates with external APIs for product enrichment.

## Features

- **Data Cleaning**: Automatically detects and removes invalid records
- **Data Validation**: Enforces business rules (positive quantities, valid IDs, etc.)
- **Sales Analysis**: Provides regional, product-wise, and customer analysis
- **API Integration**: Fetches real-time product information
- **Report Generation**: Creates comprehensive JSON and text reports
- **Error Handling**: Robust error handling and logging

## Project Structure
sales-analytics-system/
├── README.md # This file
├── main.py # Main application entry point
├── utils/ # Utility modules
│ ├── init.py
│ ├── file_handler.py # File operations and I/O
│ ├── data_processor.py # Data cleaning and analysis
│ └── api_handler.py # External API integration
├── data/ # Input data directory
│ └── sales_data.txt # Provided sales data
├── output/ # Generated output files
└── requirements.txt # Python dependencies


## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/sales-analytics-system.git
   cd sales-analytics-system

2. **Create a virtual environment (recommended):**
   python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

3. **Install dependencies:**
   pip install -r requirements.txt

## Run the main application:
python main.py
