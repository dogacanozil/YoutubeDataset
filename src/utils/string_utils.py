# Find substring between in string s between two substrings first and last
def find_between( s, first, last):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""
    
def convert_time_to_seconds(time_string):
    # Split the time string by ':'
    time_parts = list(map(int, time_string.split(':')))
    
    # Initialize total_seconds
    total_seconds = 0

    # If the format is hh:mm:ss
    if len(time_parts) == 3:
        hours, minutes, seconds = time_parts
        total_seconds = (hours * 3600) + (minutes * 60) + seconds

    # If the format is mm:ss
    elif len(time_parts) == 2:
        minutes, seconds = time_parts
        total_seconds = (minutes * 60) + seconds

    # If the format is just seconds
    elif len(time_parts) == 1:
        total_seconds = time_parts[0]

    return total_seconds

def convert_views_to_int(view_string):
    # Remove non-numeric characters
    numeric_string = ''.join(filter(str.isdigit, view_string))
    # Convert the numeric string to an integer
    return int(numeric_string)