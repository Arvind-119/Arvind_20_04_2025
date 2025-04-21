from flask import Flask, jsonify, request, send_file
import uuid
import os
from datetime import datetime
import threading
import csv
import logging

from services.report_service import ReportService
from services.data_service import DataService
from models.db import init_db, StoreStatus, BusinessHours, StoreTimezone

# Configure logging to file instead of console
logging.basicConfig(
    filename='store_monitoring.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)
reports = {}  # Store report status: {report_id: {"status": "Running"/"Complete", "file_path": path_to_csv}}

# Initialize database
logging.info("Initializing database...")
init_db()
logging.info("Database initialized")

# Initialize services
logging.info("Initializing services...")
data_service = DataService(use_minimal_logging=True)
report_service = ReportService(data_service, use_minimal_logging=True)
logging.info("Services initialized")

@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    logging.info(f"Received request to trigger report at {datetime.now()}")
    report_id = str(uuid.uuid4())
    reports[report_id] = {"status": "Running", "file_path": None}
    logging.info(f"Generated report_id: {report_id}")
    
    # Start report generation in a separate thread
    def generate_report():
        try:
            logging.info(f"Starting report generation for report_id: {report_id}")
            print(f"\n=== STARTING REPORT GENERATION: {report_id} ===")
            os.makedirs('reports', exist_ok=True)
            logging.info(f"Reports directory created/confirmed")
            output_file = os.path.join('reports', f'{report_id}.csv')
            
            # Generate the report
            logging.info(f"Calling report service to generate report: {report_id}")
            print(f"Generating report file: {output_file}")
            print(f"Current time (max timestamp in data): {data_service.current_time}")
            
            # Check if data is fully loaded
            store_count = len(data_service.get_all_store_ids())
            status_count = data_service.session.query(StoreStatus).count()
            hours_count = data_service.session.query(BusinessHours).count()
            timezone_count = data_service.session.query(StoreTimezone).count()
            
            print(f"\nDATA SUMMARY:")
            print(f"- Total stores: {store_count}")
            print(f"- Total status records: {status_count}")
            print(f"- Total business hours records: {hours_count}")
            print(f"- Total timezone records: {timezone_count}")
            
            if status_count == 0:
                print("\n⚠️ WARNING: No status data found! Report will be empty.")
            elif hours_count == 0:
                print("\n⚠️ WARNING: No business hours data found! Using 24/7 for all stores.")
            elif timezone_count == 0:
                print("\n⚠️ WARNING: No timezone data found! Using default timezone.")
                
            print("\nStarting report generation process...")
            report_service.generate_report(output_file)
            
            # Update the report status
            logging.info(f"Report generation completed for report_id: {report_id}")
            print(f"\n✅ REPORT GENERATION COMPLETED: {report_id}")
            print(f"Report saved to: {output_file}")
            
            reports[report_id] = {"status": "Complete", "file_path": output_file}
            logging.info(f"Updated report status to Complete for report_id: {report_id}")
        except Exception as e:
            logging.error(f"Error generating report {report_id}: {str(e)}")
            print(f"\n❌ ERROR GENERATING REPORT: {str(e)}")
            reports[report_id] = {"status": "Error", "error": str(e)}
    
    # Start the thread
    logging.info(f"Starting background thread for report_id: {report_id}")
    thread = threading.Thread(target=generate_report)
    thread.daemon = True
    thread.start()
    logging.info(f"Background thread started for report_id: {report_id}")
    
    return jsonify({"report_id": report_id})

@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')
    logging.info(f"Received request to get report with ID: {report_id}")
    
    if not report_id:
        logging.warning("Error: Missing report_id parameter")
        return jsonify({"error": "Missing report_id parameter"}), 400
    
    if report_id not in reports:
        logging.warning(f"Error: Report ID not found: {report_id}")
        return jsonify({"error": "Report ID not found"}), 404
    
    report_info = reports[report_id]
    logging.info(f"Report info for {report_id}: {report_info}")
    
    if report_info.get("status") == "Running":
        logging.info(f"Report {report_id} is still running")
        return jsonify({"status": "Running"})
    
    if report_info.get("status") == "Error":
        logging.error(f"Report {report_id} encountered an error: {report_info.get('error')}")
        return jsonify({"status": "Error", "error": report_info.get("error")}), 500
    
    if report_info.get("status") == "Complete":
        logging.info(f"Report {report_id} is complete, sending file: {report_info['file_path']}")
        return send_file(report_info["file_path"], as_attachment=True, download_name=f'report_{report_id}.csv')
    
    logging.warning(f"Unknown report status for {report_id}: {report_info.get('status')}")
    return jsonify({"error": "Unknown report status"}), 500

if __name__ == '__main__':
    # Ensure data is loaded on startup
    logging.info("Starting application...")
    logging.info("Loading data on startup...")
    print("Starting application - check store_monitoring.log for detailed logs")
    data_service.load_data()
    logging.info("Data loaded, starting Flask server...")
    print("Data loaded, starting Flask server on port 5000")
    app.run(debug=False, port=5000) 