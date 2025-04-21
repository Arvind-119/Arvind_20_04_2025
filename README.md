# Store Monitoring System

A system for monitoring store uptime and downtime, providing reports on store activity within business hours.

## Requirements

- Python 3.8+
- Flask
- Pandas
- SQLAlchemy
- PyTZ

## Setup

1. Create a virtual environment:
   ```
   python -m venv venv
   ```

2. Activate the virtual environment:
   - Windows:
     ```
     .\venv\Scripts\activate
     ```
   - Linux/Mac:
     ```
     source venv/bin/activate
     ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Ensure CSV data files are in the `data` directory:
   - `store_status.csv` - Contains store activity status data
   - `menu_hours.csv` - Contains business hours for stores
   - `timezones.csv` - Contains timezone information for stores

## Data Files

Before running the application, you need to ensure the following CSV files are placed in the `data/` directory:

1. `store_status.csv` - Contains store activity status data (~134MB)
   - **Important**: This file is not included in the repository due to its large size (exceeds GitHub's 100MB limit)
   - You must download it separately and place it in the `data/` directory
   
2. `menu_hours.csv` - Contains business hours for stores
   
3. `timezones.csv` - Contains timezone information for stores

Without these files properly placed in the `data/` directory, the application will not function correctly.

## Running the Application

Start the application:
```
python app.py
```

The server will run on http://localhost:5000 by default.

## API Endpoints

### 1. Trigger Report Generation

**Endpoint:** `/trigger_report`

**Method:** GET

**Response:**
```json
{
  "report_id": "unique-report-id"
}
```

### 2. Get Report Status/Download

**Endpoint:** `/get_report?report_id=<report_id>`

**Method:** GET

**Parameters:**
- `report_id`: The ID of the report to check/download

**Responses:**
- If the report is still being generated:
  ```json
  {
    "status": "Running"
  }
  ```
- If the report is complete, it will return the CSV file as a download

## Implementation Details

- The system loads data from CSV files and stores it in a SQLite database
- Reports are generated asynchronously in the background
- Uptime/downtime calculations consider business hours and timezone differences
- The system extrapolates store status based on observations for complete time intervals