import pandas as pd
import pytz
from datetime import datetime, timedelta
from sqlalchemy import func, or_
import os
import logging

from models.db import Session, StoreStatus, BusinessHours, StoreTimezone


class DataService:
    def __init__(self, use_minimal_logging=False):
        self.session = Session()
        self.default_timezone = 'America/Chicago'
        self._current_time = None  # Will be set to max timestamp in store_status
        self.use_minimal_logging = use_minimal_logging
        if not use_minimal_logging:
            print("DataService initialized")
        logging.info("DataService initialized")

    def load_data(self):
        """Load data from CSV files into the database"""
        logging.info("Starting data loading process...")
        if not self.use_minimal_logging:
            print("Starting data loading process...")
        
        # Only load if tables are empty
        status_count = self.session.query(StoreStatus).count()
        logging.info(f"Current StoreStatus records: {status_count}")
        if not self.use_minimal_logging:
            print(f"Current StoreStatus records: {status_count}")
            
        if status_count == 0:
            self._load_store_status()
        else:
            logging.info("StoreStatus data already loaded, skipping...")
            if not self.use_minimal_logging:
                print("StoreStatus data already loaded, skipping...")
        
        hours_count = self.session.query(BusinessHours).count()
        logging.info(f"Current BusinessHours records: {hours_count}")
        if not self.use_minimal_logging:
            print(f"Current BusinessHours records: {hours_count}")
            
        if hours_count == 0:
            self._load_business_hours()
        else:
            logging.info("BusinessHours data already loaded, skipping...")
            if not self.use_minimal_logging:
                print("BusinessHours data already loaded, skipping...")
        
        timezone_count = self.session.query(StoreTimezone).count()
        logging.info(f"Current StoreTimezone records: {timezone_count}")
        if not self.use_minimal_logging:
            print(f"Current StoreTimezone records: {timezone_count}")
            
        if timezone_count == 0:
            self._load_timezones()
        else:
            logging.info("StoreTimezone data already loaded, skipping...")
            if not self.use_minimal_logging:
                print("StoreTimezone data already loaded, skipping...")
        
        # Set current time to the max timestamp in store_status
        max_timestamp = self.session.query(func.max(StoreStatus.timestamp_utc)).scalar()
        self._current_time = max_timestamp
        logging.info(f"Current time set to: {self._current_time}")
        if not self.use_minimal_logging:
            print(f"Current time set to: {self._current_time}")
        
        # Print some stats for troubleshooting
        active_records = self.session.query(StoreStatus).filter(StoreStatus.status == 'active').count()
        inactive_records = self.session.query(StoreStatus).filter(StoreStatus.status == 'inactive').count()
        logging.info(f"Active status records: {active_records}, Inactive status records: {inactive_records}")
        if not self.use_minimal_logging:
            print(f"Active status records: {active_records}, Inactive status records: {inactive_records}")
        
        logging.info("Data loading process completed")
        if not self.use_minimal_logging:
            print("Data loading process completed")

    def _load_store_status(self):
        """Load store status data from CSV"""
        logging.info("Loading store status data...")
        if not self.use_minimal_logging:
            print("Loading store status data...")
            
        # Use chunksize for large file
        chunk_size = 100000
        file_path = os.path.join('data', 'store_status.csv')
        
        chunk_count = 0
        total_records = 0
        active_count = 0
        inactive_count = 0
        
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk_count += 1
            records_in_chunk = len(chunk)
            total_records += records_in_chunk
            
            # Count active/inactive statuses
            active_in_chunk = (chunk['status'] == 'active').sum()
            inactive_in_chunk = (chunk['status'] == 'inactive').sum()
            active_count += active_in_chunk
            inactive_count += inactive_in_chunk
            
            logging.info(f"Processing chunk {chunk_count} with {records_in_chunk} records (active: {active_in_chunk}, inactive: {inactive_in_chunk})...")
            if not self.use_minimal_logging:
                print(f"Processing chunk {chunk_count} with {records_in_chunk} records (active: {active_in_chunk}, inactive: {inactive_in_chunk})...")
            
            chunk['timestamp_utc'] = pd.to_datetime(chunk['timestamp_utc'], utc=True)
            
            # Convert DataFrame to list of dictionaries
            records = chunk.to_dict('records')
            
            # Bulk insert
            logging.info(f"Inserting chunk {chunk_count} into database...")
            if not self.use_minimal_logging:
                print(f"Inserting chunk {chunk_count} into database...")
                
            self.session.bulk_insert_mappings(StoreStatus, records)
            self.session.commit()
            
            logging.info(f"Chunk {chunk_count} committed to database")
            if not self.use_minimal_logging:
                print(f"Chunk {chunk_count} committed to database")
        
        logging.info(f"Store status data loaded successfully. Total records: {total_records}")
        logging.info(f"Status distribution - Active: {active_count}, Inactive: {inactive_count}")
        if not self.use_minimal_logging:
            print(f"Store status data loaded successfully. Total records: {total_records}")
            print(f"Status distribution - Active: {active_count}, Inactive: {inactive_count}")

    def _load_business_hours(self):
        """Load business hours data from CSV"""
        logging.info("Loading business hours data...")
        if not self.use_minimal_logging:
            print("Loading business hours data...")
            
        file_path = os.path.join('data', 'menu_hours.csv')
        
        df = pd.read_csv(file_path)
        records_count = len(df)
        unique_stores = df['store_id'].nunique()
        
        logging.info(f"Found {records_count} business hours records to load for {unique_stores} unique stores")
        if not self.use_minimal_logging:
            print(f"Found {records_count} business hours records to load for {unique_stores} unique stores")
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        # Bulk insert
        logging.info("Inserting business hours data into database...")
        if not self.use_minimal_logging:
            print("Inserting business hours data into database...")
            
        self.session.bulk_insert_mappings(BusinessHours, records)
        self.session.commit()
        
        logging.info(f"Business hours data loaded successfully. Total records: {records_count}")
        if not self.use_minimal_logging:
            print(f"Business hours data loaded successfully. Total records: {records_count}")

    def _load_timezones(self):
        """Load timezone data from CSV"""
        logging.info("Loading timezone data...")
        if not self.use_minimal_logging:
            print("Loading timezone data...")
            
        file_path = os.path.join('data', 'timezones.csv')
        
        df = pd.read_csv(file_path)
        records_count = len(df)
        
        logging.info(f"Found {records_count} timezone records to load")
        if not self.use_minimal_logging:
            print(f"Found {records_count} timezone records to load")
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        # Bulk insert
        logging.info("Inserting timezone data into database...")
        if not self.use_minimal_logging:
            print("Inserting timezone data into database...")
            
        self.session.bulk_insert_mappings(StoreTimezone, records)
        self.session.commit()
        
        logging.info(f"Timezone data loaded successfully. Total records: {records_count}")
        if not self.use_minimal_logging:
            print(f"Timezone data loaded successfully. Total records: {records_count}")

    def get_store_timezone(self, store_id):
        """Get timezone for a store"""
        logging.debug(f"Fetching timezone for store_id: {store_id}")
        timezone_record = self.session.query(StoreTimezone).filter_by(store_id=store_id).first()
        if timezone_record:
            logging.debug(f"Found timezone for store {store_id}: {timezone_record.timezone_str}")
            return timezone_record.timezone_str
        logging.debug(f"No timezone found for store {store_id}, using default: {self.default_timezone}")
        return self.default_timezone

    def get_business_hours(self, store_id):
        """Get business hours for a store"""
        logging.debug(f"Fetching business hours for store_id: {store_id}")
        hours = self.session.query(BusinessHours).filter_by(store_id=store_id).all()
        if hours:
            logging.debug(f"Found {len(hours)} business hour records for store {store_id}")
            # Debug: print some of the hours
            if len(hours) > 0 and not self.use_minimal_logging:
                sample = hours[0]
                logging.debug(f"Sample hours for day {sample.day_of_week}: {sample.start_time_local} to {sample.end_time_local}")
            return hours
        # If no hours found, assume 24/7
        logging.debug(f"No business hours found for store {store_id}, generating 24/7 hours")
        return self._generate_24_7_hours(store_id)

    def _generate_24_7_hours(self, store_id):
        """Generate 24/7 business hours for a store"""
        all_days = []
        for day in range(7):
            all_days.append({
                'store_id': store_id,
                'day_of_week': day,
                'start_time_local': '00:00:00',
                'end_time_local': '23:59:59'
            })
        return all_days
    
    def get_store_status_data(self, store_id, start_time, end_time):
        """Get store status data for a specific time period"""
        logging.debug(f"Fetching status data for store_id: {store_id} from {start_time} to {end_time}")
        
        # Start with the specified time range
        results = self.session.query(StoreStatus).filter(
            StoreStatus.store_id == store_id,
            StoreStatus.timestamp_utc >= start_time,
            StoreStatus.timestamp_utc <= end_time
        ).order_by(StoreStatus.timestamp_utc).all()
        
        logging.debug(f"Found {len(results)} status records for store {store_id} in the specified time range")
        
        # If we have enough data (at least 3 data points), return it
        if len(results) >= 3:
            return results
            
        # If we have at least one data point, that's better than nothing
        if len(results) > 0:
            # Check if we have at least one observation in each third of the time period
            # This would give us better coverage for interpolation
            time_range = end_time - start_time
            first_third_end = start_time + time_range / 3
            second_third_end = start_time + (time_range * 2) / 3
            
            has_first_third = False
            has_second_third = False
            has_last_third = False
            
            for record in results:
                if record.timestamp_utc <= first_third_end:
                    has_first_third = True
                elif record.timestamp_utc <= second_third_end:
                    has_second_third = True
                else:
                    has_last_third = True
            
            # If we have good coverage, no need to fetch more data
            if has_first_third and has_second_third and has_last_third:
                logging.debug(f"Good temporal coverage with {len(results)} records for store {store_id}")
                return results
        
        # If we have few or no records in the range, expand our search
        
        # First, try to get the most recent record before the range
        before_range = self.session.query(StoreStatus).filter(
            StoreStatus.store_id == store_id,
            StoreStatus.timestamp_utc < start_time
        ).order_by(StoreStatus.timestamp_utc.desc()).first()
        
        if before_range:
            logging.debug(f"Found record before range: {before_range.timestamp_utc}, status: {before_range.status}")
            # Only add if not already in results
            if before_range not in results:
                results.append(before_range)
        
        # Get the closest record after the range if necessary
        if len(results) < 3:
            after_range = self.session.query(StoreStatus).filter(
                StoreStatus.store_id == store_id,
                StoreStatus.timestamp_utc > end_time
            ).order_by(StoreStatus.timestamp_utc).first()
            
            if after_range:
                logging.debug(f"Found record after range: {after_range.timestamp_utc}, status: {after_range.status}")
                if after_range not in results:
                    results.append(after_range)
        
        # If we still have few records, expand to include more history and future data
        if len(results) < 3:
            # Look back up to 72 hours before the start time
            extended_start = start_time - timedelta(hours=72)
            logging.debug(f"Looking for additional history from {extended_start} to {start_time}")
            
            additional_history = self.session.query(StoreStatus).filter(
                StoreStatus.store_id == store_id,
                StoreStatus.timestamp_utc >= extended_start,
                StoreStatus.timestamp_utc < start_time
            ).order_by(StoreStatus.timestamp_utc.desc()).limit(5).all()
            
            if additional_history:
                logging.debug(f"Found {len(additional_history)} additional historical records")
                # Add in reverse order to maintain chronological order
                for record in reversed(additional_history):
                    if record not in results:
                        results.append(record)
            
            # Look forward up to 72 hours after the end time
            extended_end = end_time + timedelta(hours=72)
            logging.debug(f"Looking for additional future data from {end_time} to {extended_end}")
            
            additional_future = self.session.query(StoreStatus).filter(
                StoreStatus.store_id == store_id,
                StoreStatus.timestamp_utc > end_time,
                StoreStatus.timestamp_utc <= extended_end
            ).order_by(StoreStatus.timestamp_utc).limit(5).all()
            
            if additional_future:
                logging.debug(f"Found {len(additional_future)} additional future records")
                for record in additional_future:
                    if record not in results:
                        results.append(record)
        
        # Finally, as a last resort, just get any data we have for this store
        if len(results) < 2:
            logging.debug(f"Still insufficient data, fetching any available status for store {store_id}")
            any_status = self.session.query(StoreStatus).filter(
                StoreStatus.store_id == store_id
            ).order_by(StoreStatus.timestamp_utc).limit(10).all()
            
            if any_status:
                logging.debug(f"Found {len(any_status)} total records for store {store_id}")
                for record in any_status:
                    if record not in results:
                        results.append(record)
        
        # Re-sort results by timestamp
        results.sort(key=lambda x: x.timestamp_utc)
        logging.debug(f"Returning {len(results)} total status records for store {store_id}")
        
        return results
    
    def get_all_store_ids(self):
        """Get all unique store IDs"""
        logging.info("Fetching all unique store IDs...")
        # First try to get from store_status
        store_ids = self.session.query(StoreStatus.store_id).distinct().all()
        store_ids_list = [store[0] for store in store_ids]
        logging.info(f"Found {len(store_ids_list)} unique store IDs")
        
        # For testing, you could limit to a smaller sample
        # Uncomment this to process only the first 100 stores for testing
        # store_ids_list = store_ids_list[:100]
        # logging.info(f"Using a sample of {len(store_ids_list)} stores for testing")
        
        return store_ids_list
    
    @property
    def current_time(self):
        """Get the current time (max timestamp in store_status)"""
        return self._current_time 
        
    def get_latest_status_before_range(self, store_id, start_time):
        """Get the most recent status record before a given time"""
        logging.debug(f"Fetching latest status before {start_time} for store_id: {store_id}")
        
        # Get the most recent record before the specified time
        result = self.session.query(StoreStatus).filter(
            StoreStatus.store_id == store_id,
            StoreStatus.timestamp_utc < start_time
        ).order_by(StoreStatus.timestamp_utc.desc()).first()
        
        if result:
            logging.debug(f"Found status record before range: {result.timestamp_utc}, status: {result.status}")
        else:
            logging.debug(f"No status records found before {start_time} for store {store_id}")
            
        return result
        
    def get_first_timestamp(self):
        """Get the earliest timestamp in the store_status table"""
        logging.debug("Fetching first timestamp in database")
        
        # Get the minimum timestamp
        min_timestamp = self.session.query(func.min(StoreStatus.timestamp_utc)).scalar()
        
        if min_timestamp:
            logging.debug(f"First timestamp in database: {min_timestamp}")
        else:
            logging.debug("No timestamps found in database")
            
        return min_timestamp 