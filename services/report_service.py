import csv
import pytz
from datetime import datetime, timedelta
import pandas as pd
import logging
from utils.time_utils import local_time_to_utc, get_local_time, is_within_business_hours

class ReportService:
    def __init__(self, data_service, use_minimal_logging=False):
        self.data_service = data_service
        self.use_minimal_logging = use_minimal_logging
        if not use_minimal_logging:
            print("ReportService initialized")
        logging.info("ReportService initialized")
    
    def generate_report(self, output_file):
        """Generate the report with uptime/downtime metrics for all stores"""
        logging.info(f"Starting report generation, output file: {output_file}")
        if not self.use_minimal_logging:
            print(f"Starting report generation, output file: {output_file}")
            
        # Get all store IDs once
        store_ids = self.data_service.get_all_store_ids()
        total_stores = len(store_ids)
        
        if not self.use_minimal_logging:
            print(f"Generating report for {total_stores} stores")
        else:
            print(f"\n GENERATING REPORT FOR {total_stores} STORES")
            print("Progress will be shown at 10% intervals")
        
        # Validate data available for report
        # Check for first and last timestamp in data
        first_timestamp = self.data_service.get_first_timestamp()
        last_timestamp = self.data_service.current_time
        
        if first_timestamp and last_timestamp:
            print(f"\nDATA TIME RANGE:")
            print(f"- First timestamp: {first_timestamp}")
            print(f"- Last timestamp: {last_timestamp}")
            print(f"- Total time span: {last_timestamp - first_timestamp}")
            print(f"- Report will calculate metrics relative to: {last_timestamp}")
        
        # Create CSV file
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = [
                'store_id',
                'uptime_last_hour(in minutes)',
                'uptime_last_day(in hours)',
                'uptime_last_week(in hours)',
                'downtime_last_hour(in minutes)',
                'downtime_last_day(in hours)',
                'downtime_last_week(in hours)'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Process each store
            stores_processed = 0
            progress_marker = max(1, total_stores // 10)  # Show progress every 10%
            
            stats = {
                'all_zeros': 0,
                'active_stores': 0,
                'inactive_stores': 0
            }
            
            start_time = datetime.now()
            print(f"\n REPORT GENERATION STARTED AT: {start_time}")
            
            for store_id in store_ids:
                if stores_processed % 10 == 0:  # Only log every 10 stores to reduce output
                    logging.info(f"Processing store {store_id} ({stores_processed+1}/{total_stores})")
                    if not self.use_minimal_logging:
                        print(f"Processing store {store_id} ({stores_processed+1}/{total_stores})")
                
                metrics = self._calculate_metrics(store_id)
                writer.writerow(metrics)
                stores_processed += 1
                
                # Update statistics
                if (metrics['uptime_last_hour(in minutes)'] == 0 and 
                    metrics['uptime_last_day(in hours)'] == 0 and 
                    metrics['uptime_last_week(in hours)'] == 0):
                    if (metrics['downtime_last_hour(in minutes)'] == 0 and 
                        metrics['downtime_last_day(in hours)'] == 0 and 
                        metrics['downtime_last_week(in hours)'] == 0):
                        stats['all_zeros'] += 1
                    else:
                        stats['inactive_stores'] += 1
                else:
                    stats['active_stores'] += 1
                
                if stores_processed % progress_marker == 0 or stores_processed == total_stores:
                    progress_pct = (stores_processed / total_stores) * 100
                    elapsed = datetime.now() - start_time
                    avg_time_per_store = elapsed / stores_processed
                    estimated_remaining = avg_time_per_store * (total_stores - stores_processed)
                    
                    print(f"Progress: {progress_pct:.1f}% ({stores_processed}/{total_stores}) | " +
                          f"Elapsed: {elapsed} | " +
                          f"Est. remaining: {estimated_remaining}")
                    print(f"Stats so far: Active: {stats['active_stores']}, " +
                          f"Inactive: {stats['inactive_stores']}, " +
                          f"All zeros: {stats['all_zeros']}")
                    
                    # Flush the file to ensure data is written
                    csvfile.flush()
                
        end_time = datetime.now()
        total_time = end_time - start_time
        
        print(f"\n REPORT GENERATION COMPLETED")
        print(f"- Time taken: {total_time}")
        print(f"- Average time per store: {total_time / total_stores}")
        print(f"\nRESULTS SUMMARY:")
        print(f"- Active stores: {stats['active_stores']} ({stats['active_stores']/total_stores*100:.1f}%)")
        print(f"- Inactive stores: {stats['inactive_stores']} ({stats['inactive_stores']/total_stores*100:.1f}%)")
        print(f"- All zeros: {stats['all_zeros']} ({stats['all_zeros']/total_stores*100:.1f}%)")
        print(f"- Output file: {output_file}")
        
        logging.info(f"Report generation completed. Output file: {output_file}")
        if not self.use_minimal_logging:
            print(f"Report generation completed. Output file: {output_file}")
        return output_file
    
    def _calculate_metrics(self, store_id):
        """Calculate uptime/downtime metrics for a specific store"""
        logging.debug(f"Calculating metrics for store: {store_id}")
        
        # Get current time from the data service (max timestamp in data)
        current_time = self.data_service.current_time
        
        # Ensure current_time is a datetime object
        if isinstance(current_time, (int, float)):
            current_time = datetime.fromtimestamp(current_time)
        
        # Define time ranges for last hour, day, and week
        last_hour_start = current_time - timedelta(hours=1)
        last_day_start = current_time - timedelta(days=1)
        last_week_start = current_time - timedelta(days=7)
        
        # Get store timezone
        timezone_str = self.data_service.get_store_timezone(store_id)
        timezone = pytz.timezone(timezone_str)
        logging.debug(f"Store {store_id} timezone: {timezone_str}")
        
        # Get business hours for the store
        business_hours = self.data_service.get_business_hours(store_id)
        
        # Check if we have valid business hours and verify if they make sense
        has_custom_hours = True
        has_reasonable_hours = True
        if not business_hours:
            logging.debug(f"WARNING: No business hours found for store {store_id}, using 24/7")
            business_hours = self._generate_24_7_hours(store_id)
            has_custom_hours = False
        else:
            logging.debug(f"Found {len(business_hours)} business hour records for store {store_id}")
            # Check if business hours are reasonable (at least 10 minutes per day)
            total_weekly_minutes = 0
            for hours in business_hours:
                if isinstance(hours, dict):
                    start_time = hours['start_time_local']
                    end_time = hours['end_time_local']
                else:
                    start_time = hours.start_time_local
                    end_time = hours.end_time_local
                
                # Calculate minutes between start and end time
                start_parts = list(map(int, start_time.split(':')))
                end_parts = list(map(int, end_time.split(':')))
                start_minutes = start_parts[0] * 60 + start_parts[1]
                end_minutes = end_parts[0] * 60 + end_parts[1]
                
                # Handle case where end time is on the next day
                if end_minutes < start_minutes:
                    end_minutes += 24 * 60
                    
                duration = end_minutes - start_minutes
                total_weekly_minutes += duration
                
                # Log for debugging
                if duration < 10:
                    logging.debug(f"Unreasonably short hours for store {store_id}, day {hours.day_of_week if hasattr(hours, 'day_of_week') else hours['day_of_week']}: {start_time}-{end_time} ({duration} minutes)")
            
            # If store is open less than 10 hours per week in total, consider the hours data suspect
            if total_weekly_minutes < 600:  # 10 hours = 600 minutes
                logging.debug(f"WARNING: Store {store_id} has only {total_weekly_minutes} minutes of business time per week, which seems unusually low. Using standard business hours instead.")
                # Use standard business hours (9 AM to 9 PM every day) as a fallback
                business_hours = self._generate_standard_business_hours(store_id)
                has_reasonable_hours = False
            else:
                # Log a sample of business hours for debugging
                if len(business_hours) > 0:
                    sample = business_hours[0]
                    if isinstance(sample, dict):
                        logging.debug(f"Sample hours for day {sample['day_of_week']}: {sample['start_time_local']} to {sample['end_time_local']}")
                    else:
                        logging.debug(f"Sample hours for day {sample.day_of_week}: {sample.start_time_local} to {sample.end_time_local}")
        
        # Check for any status data in the past week to confirm store exists and has data
        any_status_data = self.data_service.get_store_status_data(store_id, last_week_start, current_time)
        if not any_status_data:
            logging.debug(f"WARNING: No status data found for store {store_id} in the past week")
            
            # Check if there's any historical data at all
            historical_data = self.data_service.get_latest_status_before_range(store_id, last_week_start)
            if not historical_data:
                logging.debug(f"No historical data found for store {store_id}, this may be a new or inactive store")
        else:
            logging.debug(f"Found {len(any_status_data)} status records for store {store_id} in the past week")
        
        # Calculate metrics for each time range
        logging.debug(f"Calculating last hour metrics for store {store_id}")
        last_hour_metrics = self._calculate_time_range_metrics(
            store_id, last_hour_start, current_time, business_hours, timezone, 'hour'
        )
        
        logging.debug(f"Calculating last day metrics for store {store_id}")
        last_day_metrics = self._calculate_time_range_metrics(
            store_id, last_day_start, current_time, business_hours, timezone, 'day'
        )
        
        logging.debug(f"Calculating last week metrics for store {store_id}")
        last_week_metrics = self._calculate_time_range_metrics(
            store_id, last_week_start, current_time, business_hours, timezone, 'week'
        )
        
        # Log the results for debugging
        logging.debug(f"Metrics for store {store_id}:")
        logging.debug(f"  Last hour: uptime={last_hour_metrics['uptime']}min, downtime={last_hour_metrics['downtime']}min")
        logging.debug(f"  Last day: uptime={last_day_metrics['uptime']}h, downtime={last_day_metrics['downtime']}h")
        logging.debug(f"  Last week: uptime={last_week_metrics['uptime']}h, downtime={last_week_metrics['downtime']}h")
        
        # Calculate total business time for each period to ensure correct uptime/downtime values
        total_hour_business_time = self._calculate_business_minutes_in_range(
            store_id, last_hour_start, current_time, business_hours, timezone
        )
        total_hour_business_time = min(total_hour_business_time, 60)
        
        total_day_business_time = self._calculate_business_hours_in_range(
            store_id, last_day_start, current_time, business_hours, timezone
        )
        total_day_business_time = min(total_day_business_time, 24)
        
        total_week_business_time = self._calculate_business_hours_in_range(
            store_id, last_week_start, current_time, business_hours, timezone
        )
        total_week_business_time = min(total_week_business_time, 168)
        
        # Ensure uptime values are non-negative and don't exceed total business time
        hour_uptime = max(0, min(last_hour_metrics['uptime'], total_hour_business_time))
        day_uptime = max(0, min(last_day_metrics['uptime'], total_day_business_time))
        week_uptime = max(0, min(last_week_metrics['uptime'], total_week_business_time))
        
        # Calculate downtime directly from total business time and uptime
        hour_downtime = max(0, total_hour_business_time - hour_uptime)
        day_downtime = max(0, total_day_business_time - day_uptime)
        week_downtime = max(0, total_week_business_time - week_uptime)
        
        # Combine metrics into a single dictionary
        metrics = {
            'store_id': store_id,
            'uptime_last_hour(in minutes)': round(hour_uptime, 2),
            'uptime_last_day(in hours)': round(day_uptime, 2),
            'uptime_last_week(in hours)': round(week_uptime, 2),
            'downtime_last_hour(in minutes)': round(hour_downtime, 2),
            'downtime_last_day(in hours)': round(day_downtime, 2),
            'downtime_last_week(in hours)': round(week_downtime, 2)
        }
        
        logging.debug(f"Metrics calculation completed for store {store_id}")
        return metrics
    
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
    
    def _generate_standard_business_hours(self, store_id):
        """Generate standard business hours (9 AM to 9 PM) for a store"""
        all_days = []
        for day in range(7):
            all_days.append({
                'store_id': store_id,
                'day_of_week': day,
                'start_time_local': '09:00:00',
                'end_time_local': '21:00:00'
            })
        return all_days
    
    def _calculate_time_range_metrics(self, store_id, start_time, end_time, business_hours, timezone, time_range_type):
        """Calculate uptime/downtime for a specific time range"""
        # First, calculate the total business time in the range
        total_business_time = 0
        if time_range_type == 'hour':
            total_business_time = self._calculate_business_minutes_in_range(store_id, start_time, end_time, business_hours, timezone)
            # Sanity check: total business minutes for an hour can't exceed 60
            total_business_time = min(total_business_time, 60)
        else:
            total_business_time = self._calculate_business_hours_in_range(store_id, start_time, end_time, business_hours, timezone)
            # Sanity check: total business hours for a day can't exceed 24 hours, and for a week can't exceed 168 hours
            max_hours = 24 if time_range_type == 'day' else 168
            total_business_time = min(total_business_time, max_hours)
        
        logging.debug(f"Total business time for store {store_id} in {time_range_type} range: {total_business_time}")
        
        # If no business time in this period, return zeros for both uptime and downtime
        if total_business_time <= 0:
            logging.debug(f"No business time for store {store_id} in {time_range_type} range")
            return {'uptime': 0, 'downtime': 0}
        
        # Get status data for the store in the given time range
        status_data = self.data_service.get_store_status_data(store_id, start_time, end_time)
        
        # If no data within range, try to use the last known status before this range
        # This is important for continuous monitoring
        if not status_data:
            logging.debug(f"No status data found for store {store_id} in {time_range_type} range. Looking for historical data.")
            
            # Check for data in the past week to be more thorough
            extended_start = start_time - timedelta(days=7)
            extended_status_data = self.data_service.get_store_status_data(store_id, extended_start, start_time)
            
            if extended_status_data:
                logging.debug(f"Found {len(extended_status_data)} historical status entries in the past week for store {store_id}")
                latest_status = extended_status_data[-1].status
                
                if latest_status == 'active':
                    logging.debug(f"Latest historical status was active, assuming uptime for store {store_id}")
                    return {'uptime': total_business_time, 'downtime': 0}
                else:
                    logging.debug(f"Latest historical status was inactive, assuming downtime for store {store_id}")
                    return {'uptime': 0, 'downtime': total_business_time}
            
            # If no data in past week, look for any historical data
            last_known = self.data_service.get_latest_status_before_range(store_id, start_time)
            
            if last_known:
                logging.debug(f"Found historical status: {last_known.status} at {last_known.timestamp_utc}")
                # If last known status was active, we'll assume uptime
                if last_known.status == 'active':
                    logging.debug(f"Last known status was active, assuming uptime for store {store_id}")
                    return {'uptime': total_business_time, 'downtime': 0}
                else:
                    logging.debug(f"Last known status was inactive, assuming downtime for store {store_id}")
                    return {'uptime': 0, 'downtime': total_business_time}
            else:
                # If no historical data at all, we should be more conservative
                # Look for any status data for this store in the database, even future data
                any_status = self.data_service.session.query(StoreStatus).filter_by(store_id=store_id).first()
                
                if any_status:
                    logging.debug(f"Found some status data for store {store_id}, using default assumption of active")
                    # If the store exists in the database, default to assuming it's normally active
                    # This is a more reasonable assumption as most stores are supposed to be active during business hours
                    return {'uptime': total_business_time, 'downtime': 0}
                else:
                    # If no data at all, it's safer to assume the store is down
                    logging.debug(f"No historical data found for store {store_id}, assuming downtime")
                    return {'uptime': 0, 'downtime': total_business_time}
        
        # Calculate uptime and downtime based on the status data
        logging.debug(f"Interpolating status data for store {store_id} in {time_range_type} range with {len(status_data)} records")
        uptime, downtime = self._interpolate_status(store_id, status_data, start_time, end_time, business_hours, timezone, time_range_type)
        logging.debug(f"Interpolation results for store {store_id} in {time_range_type} range: uptime={uptime}, downtime={downtime}")
        
        # Ensure uptime and downtime are non-negative
        uptime = max(0, uptime)
        downtime = max(0, downtime)
        
        # Make sure they don't exceed total business time
        if uptime > total_business_time:
            uptime = total_business_time
            downtime = 0
        elif downtime > total_business_time:
            downtime = total_business_time
            uptime = 0
        # Make sure they sum to total business time (handle rounding errors)
        elif abs((uptime + downtime) - total_business_time) > 0.1:
            logging.debug(f"Warning: Uptime ({uptime}) + Downtime ({downtime}) != Total business time ({total_business_time})")
            # Adjust downtime to make them sum correctly
            downtime = total_business_time - uptime
        
        # Final sanity check to make sure we don't have negative values
        uptime = max(0, min(uptime, total_business_time))
        downtime = max(0, min(downtime, total_business_time))
        
        # Double-check that sum equals total business time
        if abs((uptime + downtime) - total_business_time) > 0.1:
            # Force adjustment to ensure they sum to total
            ratio = total_business_time / (uptime + downtime) if uptime + downtime > 0 else 0
            uptime = uptime * ratio
            downtime = total_business_time - uptime
        
        return {'uptime': uptime, 'downtime': downtime}
    
    def _interpolate_status(self, store_id, status_data, start_time, end_time, business_hours, timezone, time_range_type):
        """Interpolate status data to calculate uptime/downtime"""
        # Convert status data to DataFrame for easier manipulation
        logging.debug(f"Converting status data to DataFrame for store {store_id}")
        df = pd.DataFrame([(s.timestamp_utc, s.status) for s in status_data], columns=['timestamp_utc', 'status'])
        
        # If no data points, we need to be more careful about assuming downtime
        if df.empty:
            logging.debug(f"Empty DataFrame for store {store_id} after conversion")
            
            # Check if we have any historical data at all for this store
            historical_data = self.data_service.get_latest_status_before_range(store_id, start_time)
            
            # If we have historical data and it's active, consider the store as active
            # Otherwise, default to treating as inactive during business hours
            if historical_data and historical_data.status == 'active':
                logging.debug(f"Found historical active status for store {store_id}, treating as active")
                # Get the total business time in this period - we'll return all uptime
                if time_range_type == 'hour':
                    business_minutes = self._calculate_business_minutes_in_range(store_id, start_time, end_time, business_hours, timezone)
                    # Cap at 60 minutes for an hour
                    business_minutes = min(business_minutes, 60)
                    return business_minutes, 0  # All uptime
                else:
                    # Calculate total business hours and cap at reasonable limits
                    business_hours_in_range = self._calculate_business_hours_in_range(store_id, start_time, end_time, business_hours, timezone)
                    max_hours = 24 if time_range_type == 'day' else 168
                    business_hours_in_range = min(business_hours_in_range, max_hours)
                    return business_hours_in_range, 0  # All uptime
            else:
                # Instead of defaulting to inactive, check for any status data for this store
                any_status = self.data_service.session.query(StoreStatus).filter_by(store_id=store_id).first()
                
                if any_status:
                    logging.debug(f"Found some status data for store {store_id}, assuming active during business hours by default")
                    # Get the total business time in this period - we'll return all uptime
                    if time_range_type == 'hour':
                        business_minutes = self._calculate_business_minutes_in_range(store_id, start_time, end_time, business_hours, timezone)
                        # Cap at 60 minutes for an hour
                        business_minutes = min(business_minutes, 60)
                        return business_minutes, 0  # Default to uptime
                    else:
                        # Calculate total business hours and cap at reasonable limits
                        business_hours_in_range = self._calculate_business_hours_in_range(store_id, start_time, end_time, business_hours, timezone)
                        max_hours = 24 if time_range_type == 'day' else 168 
                        business_hours_in_range = min(business_hours_in_range, max_hours)
                        return business_hours_in_range, 0  # Default to uptime
                else:
                    logging.debug(f"No historical data or inactive status for store {store_id}, treating as inactive")
                    # Get the total business time in this period - we'll return all downtime
                    if time_range_type == 'hour':
                        business_minutes = self._calculate_business_minutes_in_range(store_id, start_time, end_time, business_hours, timezone)
                        # Cap at 60 minutes for an hour
                        business_minutes = min(business_minutes, 60)
                        return 0, business_minutes  # All downtime
                    else:
                        # Calculate total business hours and cap at reasonable limits
                        business_hours_in_range = self._calculate_business_hours_in_range(store_id, start_time, end_time, business_hours, timezone)
                        max_hours = 24 if time_range_type == 'day' else 168 
                        business_hours_in_range = min(business_hours_in_range, max_hours)
                        return 0, business_hours_in_range  # All downtime
        
        # Add start and end times if they are not in the data
        if df['timestamp_utc'].min() > start_time:
            logging.debug(f"Adding start time to DataFrame for store {store_id}")
            # Use the status of the first data point for the start time
            first_status = df.iloc[0]['status']
            df = pd.concat([pd.DataFrame([{'timestamp_utc': start_time, 'status': first_status}]), df])
        
        if df['timestamp_utc'].max() < end_time:
            logging.debug(f"Adding end time to DataFrame for store {store_id}")
            # Use the status of the last data point for the end time
            last_status = df.iloc[-1]['status']
            df = pd.concat([df, pd.DataFrame([{'timestamp_utc': end_time, 'status': last_status}])])
        
        # Sort by timestamp
        df = df.sort_values('timestamp_utc')
        
        logging.debug(f"Calculating intervals between observations for store {store_id}")
        # Calculate time intervals between consecutive observations
        df['next_timestamp'] = df['timestamp_utc'].shift(-1)
        df['interval'] = (df['next_timestamp'] - df['timestamp_utc']).dt.total_seconds()
        
        # Filter out the last row (which has NaN for next_timestamp)
        df = df[:-1]
        
        # Initialize uptime and downtime counters based on time_range_type
        uptime = 0
        downtime = 0
        
        # If we have long gaps between observations (more than 3 hours),
        # we should handle them specially - stores are likely active during business hours
        max_reasonable_gap = 3 * 60 * 60  # 3 hours in seconds
        
        # Calculate total business time for the entire range
        if time_range_type == 'hour':
            total_business_time = self._calculate_business_minutes_in_range(store_id, start_time, end_time, business_hours, timezone)
            # Cap at 60 minutes for an hour
            total_business_time = min(total_business_time, 60)
        else:
            # Calculate total business hours and cap at reasonable limits
            total_business_time = self._calculate_business_hours_in_range(store_id, start_time, end_time, business_hours, timezone)
            max_hours = 24 if time_range_type == 'day' else 168
            total_business_time = min(total_business_time, max_hours)
        
        logging.debug(f"Processing {len(df)} intervals for store {store_id}")
        # Process each interval
        for idx, row in df.iterrows():
            start_interval = row['timestamp_utc']
            end_interval = row['next_timestamp']
            status = row['status']
            interval_length = row['interval']
            
            # Special handling for long gaps in data
            if interval_length > max_reasonable_gap:
                logging.debug(f"Long gap detected for store {store_id}: {interval_length/3600:.2f} hours")
                
                # For long gaps, we handle business hours and non-business hours differently
                # For non-business hours, we use the normal interval calculation
                
                # Convert interval start/end to local time to check business hours
                local_start = get_local_time(start_interval, timezone)
                local_end = get_local_time(end_interval, timezone)
                
                # Check if start and end are in business hours
                start_in_business = self._is_business_time(local_start, business_hours)
                end_in_business = self._is_business_time(local_end, business_hours)
                
                # If both are in business hours, adjust calculation to favor uptime during business hours
                if start_in_business and end_in_business:
                    logging.debug(f"Long gap during business hours, adjusting calculation to favor uptime")
                    
                    # Assume active during business hours unless proven otherwise
                    if status == 'active':
                        interval_uptime, interval_downtime = self._calculate_interval_metrics(
                            store_id, start_interval, end_interval, 'active', business_hours, timezone, time_range_type
                        )
                    else:
                        # If current status is inactive, split the interval:
                        # - First hour maintains the inactive status
                        # - Rest of the time during business hours assumed to be active
                        first_part_end = start_interval + timedelta(hours=1)
                        second_part_start = first_part_end
                        
                        # Calculate for first part (inactive)
                        first_uptime, first_downtime = self._calculate_interval_metrics(
                            store_id, start_interval, first_part_end, 'inactive', business_hours, timezone, time_range_type
                        )
                        
                        # Calculate for second part (assume active during business hours)
                        second_uptime, second_downtime = self._calculate_interval_metrics(
                            store_id, second_part_start, end_interval, 'active', business_hours, timezone, time_range_type
                        )
                        
                        interval_uptime = first_uptime + second_uptime
                        interval_downtime = first_downtime + second_downtime
                else:
                    # Regular calculation for non-business hours
                    interval_uptime, interval_downtime = self._calculate_interval_metrics(
                        store_id, start_interval, end_interval, status, business_hours, timezone, time_range_type
                    )
            else:
                # For normal intervals, just use the standard calculation
                interval_uptime, interval_downtime = self._calculate_interval_metrics(
                    store_id, start_interval, end_interval, status, business_hours, timezone, time_range_type
                )
            
            uptime += interval_uptime
            downtime += interval_downtime
        
        # Ensure uptime and downtime don't exceed total business time
        uptime = min(uptime, total_business_time)
        downtime = min(downtime, total_business_time)
        
        # If the sum exceeds total_business_time, scale them down proportionally
        if uptime + downtime > total_business_time and (uptime + downtime) > 0:
            ratio = total_business_time / (uptime + downtime)
            uptime *= ratio
            downtime *= ratio
            
        logging.debug(f"Total uptime/downtime for store {store_id}: uptime={uptime}, downtime={downtime}")
        return uptime, downtime
    
    def _calculate_interval_metrics(self, store_id, start_interval, end_interval, status, business_hours, timezone, time_range_type):
        """Calculate uptime/downtime for a specific interval"""
        # Safety check - ensure that end_interval is after start_interval
        if end_interval <= start_interval:
            logging.debug(f"Invalid interval for store {store_id}: end {end_interval} <= start {start_interval}")
            return 0, 0
            
        # Convert interval to local time
        local_start = get_local_time(start_interval, timezone)
        local_end = get_local_time(end_interval, timezone)
        
        # Calculate total business time in this interval
        business_time = self._calculate_business_time_in_interval(store_id, local_start, local_end, business_hours, time_range_type)
        
        # Apply reasonable caps on the business time based on the interval length
        interval_length_seconds = (end_interval - start_interval).total_seconds()
        
        if time_range_type == 'hour':
            # Convert seconds to minutes for hourly calculation
            max_business_time = min(interval_length_seconds / 60, 60)
            business_time = min(business_time, max_business_time)
        else:
            # Convert seconds to hours for daily/weekly calculation
            max_business_time = interval_length_seconds / 3600
            
            # Add additional caps based on time range type
            if time_range_type == 'day':
                max_business_time = min(max_business_time, 24)
            elif time_range_type == 'week':
                max_business_time = min(max_business_time, 168)
                
            business_time = min(business_time, max_business_time)
        
        # If status is active, add to uptime, otherwise to downtime
        if status == 'active':
            return business_time, 0
        else:
            return 0, business_time
    
    def _calculate_business_time_in_interval(self, store_id, local_start, local_end, business_hours, time_range_type):
        """Calculate business time in a given interval"""
        # Initialize counters
        business_time = 0
        
        # Safety check - ensure that local_end is after local_start
        if local_end <= local_start:
            logging.debug(f"Invalid local time interval for store {store_id}: end {local_end} <= start {local_start}")
            return 0
            
        # Calculate maximum possible time in this interval
        max_possible_time = (local_end - local_start).total_seconds()
        if time_range_type == 'hour':
            # Convert to minutes for hourly calculation
            max_possible_time /= 60
            # Cap at 60 minutes for hour calculations
            max_possible_time = min(max_possible_time, 60)
        else:
            # Convert to hours for daily/weekly calculation
            max_possible_time /= 3600
            # Apply caps based on time range type
            if time_range_type == 'day':
                max_possible_time = min(max_possible_time, 24)
            elif time_range_type == 'week':
                max_possible_time = min(max_possible_time, 168)
        
        # Current time pointer
        current = local_start
        
        # Iterate through the interval in 15-minute increments
        increment = timedelta(minutes=15)
        
        # Prevent excessive iterations for long intervals
        max_iterations = 1000  # This is a safety limit to prevent infinite loops
        iteration_count = 0
        
        while current < local_end and iteration_count < max_iterations:
            next_time = min(current + increment, local_end)
            
            # Check if midpoint of this 15-min segment is within business hours
            midpoint = current + (next_time - current) / 2
            is_business = self._is_business_time(midpoint, business_hours)
            
            if is_business:
                # Calculate the duration in appropriate units
                duration_seconds = (next_time - current).total_seconds()
                
                if time_range_type == 'hour':
                    # Convert to minutes
                    business_time += duration_seconds / 60
                else:
                    # Convert to hours
                    business_time += duration_seconds / 3600
            
            # Move to next 15-min segment
            current = next_time
            iteration_count += 1
        
        # Ensure the business time doesn't exceed the maximum possible time
        business_time = min(business_time, max_possible_time)
        
        return business_time
    
    def _is_business_time(self, local_time, business_hours):
        """Check if a given local time is within business hours"""
        # Get day of week (0=Monday, 6=Sunday)
        day_of_week = local_time.weekday()
        
        # Convert time to string format for comparison
        time_str = local_time.strftime('%H:%M:%S')
        
        # Flag to check if we found valid business hours for this day
        valid_hours_for_day = False
        unreasonable_hours = False
        unusual_hours = False
        
        # Check if time is within any business hour for this day
        for hours in business_hours:
            if isinstance(hours, dict):  # Handle both dict and object formats
                if hours['day_of_week'] == day_of_week:
                    valid_hours_for_day = True
                    start_time = hours['start_time_local']
                    end_time = hours['end_time_local']
                    
                    # Check for unusual hours (like ending at 23:59:59)
                    if end_time == '23:59:59':
                        unusual_hours = True
                    
                    # Calculate minutes between start and end time
                    start_parts = list(map(int, start_time.split(':')))
                    end_parts = list(map(int, end_time.split(':')))
                    start_minutes = start_parts[0] * 60 + start_parts[1]
                    end_minutes = end_parts[0] * 60 + end_parts[1]
                    
                    # Handle case where end time is on the next day (crossing midnight)
                    if end_minutes < start_minutes:
                        # For overnight hours, we need to check both sides of midnight
                        current_time_parts = list(map(int, time_str.split(':')))
                        current_minutes = current_time_parts[0] * 60 + current_time_parts[1]
                        
                        # Check if current time is between start and midnight
                        if start_minutes <= current_minutes:
                            return True
                        
                        # Check if current time is between midnight and end
                        if current_minutes <= end_minutes:
                            return True
                    
                    # Check if business hours are unreasonably short (less than 10 minutes)
                    if end_minutes - start_minutes < 10:
                        logging.debug(f"Unreasonably short business hours detected: {start_time} to {end_time} (only {end_minutes - start_minutes} minutes)")
                        unreasonable_hours = True
                        # Don't return yet, check if there are other business hours for this day
                    else:
                        # Normal case - check if current time is within business hours
                        # Use a more flexible comparison for edge cases
                        if start_time <= time_str <= end_time:
                            return True
            else:  # Same logic for object format
                if hours.day_of_week == day_of_week:
                    valid_hours_for_day = True
                    start_time = hours.start_time_local
                    end_time = hours.end_time_local
                    
                    # Check for unusual hours (like ending at 23:59:59)
                    if end_time == '23:59:59':
                        unusual_hours = True
                    
                    # Calculate minutes between start and end time
                    start_parts = list(map(int, start_time.split(':')))
                    end_parts = list(map(int, end_time.split(':')))
                    start_minutes = start_parts[0] * 60 + start_parts[1]
                    end_minutes = end_parts[0] * 60 + end_parts[1]
                    
                    # Handle case where end time is on the next day
                    if end_minutes < start_minutes:
                        # For overnight hours, we need to check both sides of midnight
                        current_time_parts = list(map(int, time_str.split(':')))
                        current_minutes = current_time_parts[0] * 60 + current_time_parts[1]
                        
                        # Check if current time is between start and midnight
                        if start_minutes <= current_minutes:
                            return True
                        
                        # Check if current time is between midnight and end
                        if current_minutes <= end_minutes:
                            return True
                    
                    # Check if business hours are unreasonably short
                    if end_minutes - start_minutes < 10:
                        logging.debug(f"Unreasonably short business hours detected: {start_time} to {end_time} (only {end_minutes - start_minutes} minutes)")
                        unreasonable_hours = True
                    else:
                        if start_time <= time_str <= end_time:
                            return True
        
        # If we found business hours for this day but they were all unreasonably short,
        # assume this is a data issue and treat as if the store is open for standard hours
        if valid_hours_for_day and unreasonable_hours:
            # Check if current time is between 9 AM and 9 PM as a reasonable default
            current_hour = local_time.hour
            if 9 <= current_hour < 21:  # 9 AM to 9 PM
                logging.debug(f"Using default 9-21 hours due to unreasonable business hours")
                return True
        
        # If hours end at 23:59:59, it might be a day where the store is open 24 hours
        # Be more generous with the interpretation
        if valid_hours_for_day and unusual_hours:
            # If any hours end at 23:59:59, check if there's also a corresponding early morning start
            morning_start = False
            for hours in business_hours:
                if isinstance(hours, dict):
                    if hours['day_of_week'] == day_of_week:
                        start_time = hours['start_time_local']
                        start_parts = list(map(int, start_time.split(':')))
                        if start_parts[0] < 6:  # Starts before 6 AM
                            morning_start = True
                            break
                else:
                    if hours.day_of_week == day_of_week:
                        start_time = hours.start_time_local
                        start_parts = list(map(int, start_time.split(':')))
                        if start_parts[0] < 6:  # Starts before 6 AM
                            morning_start = True
                            break
            
            # If we have both late closing and early opening, it might indicate 24-hour operation
            if morning_start:
                logging.debug(f"Detected possible 24-hour operation based on unusual hours")
                return True
        
        # If no explicit business hours for today, but the store has business hours for other days,
        # use the most common hours as a fallback
        if not valid_hours_for_day and business_hours:
            common_hours = {}
            for hours in business_hours:
                if isinstance(hours, dict):
                    key = f"{hours['start_time_local']}-{hours['end_time_local']}"
                    common_hours[key] = common_hours.get(key, 0) + 1
                else:
                    key = f"{hours.start_time_local}-{hours.end_time_local}"
                    common_hours[key] = common_hours.get(key, 0) + 1
            
            if common_hours:
                # Find most common hours
                most_common = max(common_hours.items(), key=lambda x: x[1])
                most_common_key = most_common[0]
                start_time, end_time = most_common_key.split('-')
                
                logging.debug(f"No hours for day {day_of_week}, using most common hours: {start_time}-{end_time}")
                
                # Check if current time is within these hours
                if start_time <= time_str <= end_time:
                    return True
        
        return False
    
    def _calculate_business_minutes_in_range(self, store_id, start_time, end_time, business_hours, timezone):
        """Calculate total business minutes in a time range"""
        logging.debug(f"Calculating business minutes in range for store {store_id}")
        # For hour calculation, we need minutes
        business_minutes = 0
        
        # Safety check - ensure that end_time is after start_time
        if end_time <= start_time:
            logging.debug(f"Invalid time range for store {store_id}: end time {end_time} <= start time {start_time}")
            return 0
            
        # Calculate the maximum possible minutes in this range
        max_possible_minutes = int((end_time - start_time).total_seconds() / 60)
        
        # Convert to local timezone
        local_start = get_local_time(start_time, timezone)
        local_end = get_local_time(end_time, timezone)
        
        # Calculate business minutes
        current = local_start
        increment = timedelta(minutes=1)
        
        # Prevent infinite loops by setting a reasonable limit
        iteration_limit = min(max_possible_minutes, 24 * 60)  # At most 24 hours of computation
        iteration_count = 0
        
        while current < local_end and iteration_count < iteration_limit:
            if self._is_business_time(current, business_hours):
                business_minutes += 1
            current += increment
            iteration_count += 1
        
        # The total business minutes for a limited time range can't exceed the time range itself
        business_minutes = min(business_minutes, max_possible_minutes)
        
        # For an hour range, cap at 60 minutes
        if (end_time - start_time) <= timedelta(hours=1):
            business_minutes = min(business_minutes, 60)
            
        logging.debug(f"Total business minutes in range for store {store_id}: {business_minutes} (max possible: {max_possible_minutes})")
        return business_minutes
    
    def _calculate_business_hours_in_range(self, store_id, start_time, end_time, business_hours, timezone):
        """Calculate total business hours in a time range"""
        logging.debug(f"Calculating business hours in range for store {store_id}")
        # For day/week calculation, we need hours
        business_hours_count = 0
        
        # Safety check - ensure that end_time is after start_time
        if end_time <= start_time:
            logging.debug(f"Invalid time range for store {store_id}: end time {end_time} <= start time {start_time}")
            return 0
            
        # Calculate the maximum possible hours in this range
        max_possible_hours = (end_time - start_time).total_seconds() / 3600
        
        # Convert to local timezone
        local_start = get_local_time(start_time, timezone)
        local_end = get_local_time(end_time, timezone)
        
        # Calculate business hours
        current = local_start
        increment = timedelta(minutes=60)
        
        # Use a more reasonable increment that won't result in too many iterations
        # If the range is large (over a week), use larger increments
        if (end_time - start_time) > timedelta(days=7):
            increment = timedelta(hours=3)
        elif (end_time - start_time) > timedelta(days=1):
            increment = timedelta(hours=1)
        else:
            increment = timedelta(minutes=30)
            
        # Prevent infinite loops by setting a reasonable limit (1 week = 168 hours max)
        iteration_limit = 24 * 7 * 2  # 2x the number of half hours in a week
        iteration_count = 0
        
        while current < local_end and iteration_count < iteration_limit:
            next_hour = min(current + increment, local_end)
            midpoint = current + (next_hour - current) / 2
            
            if self._is_business_time(midpoint, business_hours):
                # Calculate the fraction of an hour
                fraction = (next_hour - current).total_seconds() / 3600
                business_hours_count += fraction
            
            current = next_hour
            iteration_count += 1
        
        # The total business hours can't exceed the time range itself
        business_hours_count = min(business_hours_count, max_possible_hours)
        
        # Apply reasonable caps based on time period
        if (end_time - start_time) <= timedelta(days=1):
            # For a day range, cap at 24 hours
            business_hours_count = min(business_hours_count, 24)
        elif (end_time - start_time) <= timedelta(days=7):
            # For a week range, cap at 168 hours (7 days)
            business_hours_count = min(business_hours_count, 168)
            
        logging.debug(f"Total business hours in range for store {store_id}: {business_hours_count} (max possible: {max_possible_hours})")
        return business_hours_count 