# USE A DICTIONARY INSTEAD
import sys
import csv
from datetime import datetime
from time import perf_counter_ns
from collections import Counter
# Features:
# timestamp	user_id	pixel_color	coordinate
#  2022-04-04 00:53:51.577 UTC  #000000  8261048
def mostPlacedColor(color_list):
    # Return the most placed color
    color_dict = {}
    for color in color_list:
        # If color already exists
        if color in color_dict:
            # Get the value
            new_val = color_dict.get(color)
            color_dict[color] = new_val + 1
        else:
            # Put in dict with val 1
            color_dict[color] = 1
    return max(color_dict, key = lambda x: color_dict[x])
def mostPlacedPixel(coord_list):
    # Return the most placed pixel coord
    coord_dict = {}
    for coord in coord_list:
        # If color already exists
        if coord in coord_dict:
            # Get the value
            new_val = coord_dict.get(coord)
            coord_dict[coord] = new_val + 1
        else:
            # Put in dict with val 1
            coord_dict[coord] = 1
    return max(coord_dict, key = lambda x: coord_dict[x])
def date_format(arg):
    try:
        d = datetime.strptime(arg, "%Y-%m-%d %H")
        return d
    except ValueError:
        # Exit on invalid format
        print("Invalid date format")
        sys.exit(1)
# Check if timestamp has ms, if not go to basic
def ts_format_ms(arg):
    try:
        d = datetime.strptime(arg, "%Y-%m-%d %H:%M:%S.%f %Z")
        return d
    except ValueError:
        # Check if timestamp is in format with no ms
        try:
            d = datetime.strptime(arg, "%Y-%m-%d %H:%M:%S %Z")
            return d
        except ValueError:
            print("Invalid date format")
            sys.exit(1)


    
def main():
    # Timer
    time_start = perf_counter_ns()
    color_list = []
    coord_list = []
    # Handle Command Line Args
    ''' 
    From command line, user enters 2 args:
        Format: YYYY-MM-DD HH 
    '''
    # Check for correct format
    start = sys.argv[1]
    start = date_format(start)
    end = sys.argv[2]
    end = date_format(end)
    # Validate End Date is After Start Date
    if start > end:
        print("Start date is after end date")
        sys.exit(1)
    # Validate start date is not after 2022
    if start.year > 2022:
        print("Start date is after 2022")
        sys.exkt(1)

    # Open 2022_place_canvas_history.csv
    with open('2022_place_canvas_history.csv', 'r') as f:
        csv_read = csv.reader(f)
        # Skip header row
        next(csv_read)
        #i = 0
        for row in csv_read:
            #if i % 1000000 == 0:
                #print("Another 1M lines read", i)
            #i += 1
            # Only add to list if within timeframe
            # Do i have to validate every time?
            #curr_date = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f %Z")
            curr_date = ts_format_ms(row[0])
            if curr_date >= start and curr_date <= end: 
                #if curr_date >= end:
                    #break               
                color_list.append(row[2])
                coord_list.append(row[3])
       
    # Call functions
    print("Most placed pixel color:", mostPlacedColor(color_list))
    print("Most placed pixel location:", mostPlacedPixel(coord_list))
    # Stop timer
    stop = perf_counter_ns()
    print("Time:", (stop-time_start) / 1000000, 'ms')
    return
if __name__ == '__main__':
    # Make sure a start and end time are entered in CLI
    if len(sys.argv) != 3:
        print("Not enough args entered")
        print("Args: file, start date, end date")
        sys.exit(1) # Not normal exit
    main()

# Checklist
'''
- Can't use pandas or numpy
- Not enough args entered error DONE
- Invalid date format entered error DONE
- Start date is after end date DONE
- File stops reading after out of time range DONE
- Timer DONE
'''
