import pytz
from datetime import datetime, time

def get_local_time(utc_time, timezone):
    """Convert UTC time to local time"""
    # Handle case where utc_time might be an integer timestamp instead of datetime object
    if isinstance(utc_time, (int, float)):
        utc_time = datetime.fromtimestamp(utc_time, pytz.UTC)
    elif utc_time.tzinfo is None:
        utc_time = pytz.utc.localize(utc_time)
    return utc_time.astimezone(timezone)

def local_time_to_utc(local_time, timezone):
    """Convert local time to UTC time"""
    # Handle case where local_time might be an integer timestamp instead of datetime object
    if isinstance(local_time, (int, float)):
        local_time = datetime.fromtimestamp(local_time, timezone)
    elif local_time.tzinfo is None:
        local_time = timezone.localize(local_time)
    return local_time.astimezone(pytz.utc)

def parse_time_str(time_str):
    """Parse a time string (HH:MM:SS) to a time object"""
    hours, minutes, seconds = map(int, time_str.split(':'))
    return time(hours, minutes, seconds)

def is_within_business_hours(local_time, business_hours):
    """Check if a local time is within business hours"""
    # Handle case where local_time might be an integer timestamp
    if isinstance(local_time, (int, float)):
        local_time = datetime.fromtimestamp(local_time)
        
    # Get day of week (0=Monday, 6=Sunday)
    day_of_week = local_time.weekday()
    
    # Get time component
    time_only = local_time.time()
    
    # Check if the time is within business hours for this day
    for hours in business_hours:
        if isinstance(hours, dict):
            if hours['day_of_week'] == day_of_week:
                start_time = parse_time_str(hours['start_time_local'])
                end_time = parse_time_str(hours['end_time_local'])
                if start_time <= time_only <= end_time:
                    return True
        else:
            if hours.day_of_week == day_of_week:
                start_time = parse_time_str(hours.start_time_local)
                end_time = parse_time_str(hours.end_time_local)
                if start_time <= time_only <= end_time:
                    return True
    
    return False 