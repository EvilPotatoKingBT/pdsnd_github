import time
import pandas as pd
import numpy as np
import glob
import os
from pathlib import Path
import timeit
from collections import Counter
from itertools import groupby

my_folder = None

def load_and_merge_bikeshare_data():
    
    time_start = timeit.default_timer()
    
    #instead of loading each bikeshare csv after each restart, i want to load them all into one file at the start
    #and then simply access data based on specified criteria.
    
    #getting current folder (in which CSVs are also located)
    #if they are located in some 'data' folder, it can be hardcoded
    #https://docs.python.org/3.0/library/os.html
    my_folder = os.getcwd()

    #getting all valid CSVs in one list
    #https://stackoverflow.com/questions/33747968/getting-file-list-using-glob-in-python
    file_list = glob.glob(my_folder + '/*.csv') # Include slash or it will search in the wrong directory!!
    #print('file_list {}'.format(file_list))
    
    #access each file, then adding namefile to designated column so data can be filtered by city later
    #https://stackoverflow.com/questions/51845613/adding-columns-to-dataframe-based-on-file-name-in-python
    data_all = pd.DataFrame()
    for file_name in file_list:
        tmp = pd.read_csv(file_name)
        
        my_name = Path(file_name).stem      #https://stackoverflow.com/questions/678236/how-to-get-the-filename-without-the-extension-from-a-path-in-python
        tmp['city_name'] = my_name
        
        #assign numerical value to cities with '-1' (int) being a placeholder with no current use
        #'0' (int) is reserved for all data (no filter applied)
        if my_name == 'chicago':
            tmp['city_id'] = 1
        elif my_name == 'new_york_city':
            tmp['city_id'] = 2
        elif my_name == 'washington':
            tmp['city_id'] = 3
        else:
            tmp['city_id'] = -1
        
        #convert 'Start Time' column to datetime
        tmp['Start Time'] = pd.to_datetime(tmp['Start Time'])
        tmp['month'] = tmp['Start Time'].dt.month
        tmp['month_name'] = tmp['Start Time'].dt.month_name()
         
        tmp['weekday'] = tmp['Start Time'].dt.weekday       # 0 = Mon, 1 = Tue, 2 = Wed, 3 = Thu, 4 = Fri, 5 = Sat, 6 = Sun
        tmp['weekday_name'] = tmp['Start Time'].dt.weekday_name
        
        tmp['hour'] = tmp['Start Time'].dt.hour
        
        data_all = data_all.append(tmp, sort=False)
        
    data_all = data_all.reset_index()
    
    #change unspecified gender to 'Unknown'
    #data_all['Gender'] = data_all['Gender'].fillna('Unknown')
    #data_all['Gender'] = data_all['Gender'].dropna()
    
    #change unsecified or missing 'Birth Year' to 'Unknown'
    #this messes up later calculation. ommitted for now
    #data_all['Birth Year'] = data_all['Birth Year'].fillna('Unknown')
    #data_all['Birth Year'] = data_all['Birth Year'].dropna()
    
    #add indentifier for full trips 'Start Station' + '_' + 'End Station'
    data_all['full_trip'] = data_all['Start Station'] + ' _ ' + data_all['End Station']
    
    #time_end = timeit.default_timer()
    time_spent = round(timeit.default_timer() - time_start, 3)
    print('Bikeshare data for all cities available are loaded. This took: ' + str(time_spent) + ' seconds.\n')
    
    return data_all


def get_filters():
    '''
    Asks user to specify a city, month, and day to analyze.
    
    Returns:
        (str) city - name of the city to analyze
        (str) month - name of the month to filter by, or 'all' to apply no month filter
        (str) day - name of the day of week to filter by, or 'all' to apply no day filter
    '''
    
    print('Hello! Let\'s explore some US bikeshare data!')
    
    #city selection:
    city_print = None
    while True:
        print('\n')
        my_input = input('Which city do you want to see information for?\n0 - All, 1 - Chicago, 2 - New York, 3 - Washington\nYou can use either number or city name:   ')
    
        if my_input == '0' or my_input.lower() in ('all'):
            city = 0
            city_print = 'All'
        elif my_input == '1' or my_input.lower() == 'chicago' or my_input.lower() == 'chi':
            city = 'chicago'
            city_print = 'Chicago'
        elif my_input == '2' or my_input.lower() == 'new york' or my_input.lower() == 'ny' or my_input.lower() == 'nyc':
            city = 'new_york_city'
            city_print = 'New York'
        elif my_input == '3' or my_input.lower() == 'washington' or my_input.lower() == 'wash':
            city = 'washington'
            city_print = 'Washington'
        else:
            city = None
            city_print = None
        
        if city == None:
            print('Your selection is invalid, please select valid city.')
        else:
            break
    
    #month selection:
    month_print = None
    while True:
        print('\n')
        my_input = input('Please select Month for which you want to see the data:\n0 - All, 1 - January, 2 - February, 3 - March, 4 - April, 5 - May, 6 - June\nYou can use either number or month name:   ')
    
        if my_input == '0' or my_input.lower() in ('all'):
            month = 0
            month_print = 'All'
        elif my_input == '1' or my_input.lower() in ('january', 'jan'):
            month = 1
            month_print = 'January'
        elif my_input == '2' or my_input.lower() in ('february', 'feb'):
            month = 2
            month_print = 'February'
        elif my_input == '3' or my_input.lower() in ('march', 'mar'):
            month = 3
            month_print = 'March'
        elif my_input == '4' or my_input.lower() in ('april', 'apr'):
            month = 4
            month_print = 'April'
        elif my_input == '5' or my_input.lower() in ('may'):
            month = 5
            month_print = 'May'
        elif my_input == '6' or my_input.lower() in ('june', 'jun'):
            month = 6
            month_print = 'June'
        else:
            month = None
            month_print = None
        
        if month == None:
            print('Your selection is invalid, please select valid month or all months.')
        else:
            break
        
    #weekday selection:
    weekday_print = None
    while True:
        print('\n')
        my_input = input('Please select day of the week for which you want to see the data:\n0 - All, 1 - Mon, 2 - Tue, 3 - Wed, 4 - Thu, 5 - Fri, 6 - Sat, 7 - Sun\nYou can use either number or month name:   ')
    
        if my_input == '0' or my_input.lower() in ('all'):
            weekday = -1
            weekday_print = 'All'
        elif my_input == '1' or my_input.lower() in ('mon', 'monday'):
            weekday = 0
            weekday_print = 'Mon'
        elif my_input == '2' or my_input.lower() in ('tue', 'tuesday'):
            weekday = 1
            weekday_print = 'Tue'
        elif my_input == '3' or my_input.lower() in ('wed', 'wednesday'):
            weekday = 2
            weekday_print = 'Wed'
        elif my_input == '4' or my_input.lower() in ('thu', 'thursday'):
            weekday = 3
            weekday_print = 'Thu'
        elif my_input == '5' or my_input.lower() in ('fri', 'friday'):
            weekday = 4
            weekday_print = 'Fri'
        elif my_input == '6' or my_input.lower() in ('sat', 'saturday'):
            weekday = 5
            weekday_print = 'Sat'
        elif my_input == '7' or my_input.lower() in ('sun', 'sunday'):
            weekday = 6
            weekday_print = 'Sun'
        else:
            weekday = None
            weekday_print = None
        
        if weekday == None:
            print('Your selection is invalid, please select valid day of the week.')
        else:
            break
    
    print('-'*40 + '\n')
    print('You chose: ' + city_print + ', ' + month_print + ', ' + weekday_print + '.\n')    
    return city, month, weekday

def apply_filters(data_all, city, month, weekday):
    
    time_start = timeit.default_timer()
    if city == 0 and month == 0 and weekday == -1:                                                      #No filter
        data_filtered = data_all
    elif city != 0 and month == 0 and weekday == -1:                                                    #Only filtering by City
        data_filtered = data_all[(data_all['city_name'] == city)]
    elif city == 0 and month != 0 and weekday == -1:                                                    #Only filtering by month
        data_filtered = data_all[(data_all['month'] == month)]
    elif city == 0 and month == 0 and weekday != -1:                                                    #Only filtering by weekday
        data_filtered = data_all[(data_all['weekday'] == weekday)]
    elif city == 0 and month != 0 and weekday != -1:                                                    #There is no filter present for City
        data_filtered = data_all[(data_all['city_name'] == city) & (data_all['month'] == month)]
    elif city != 0 and month != 0 and weekday == -1:                                                    #There is no filter present for month
        data_filtered = data_all[(data_all['city_name'] == city) & (data_all['month'] == month)]
    elif city != 0 and month == 0 and weekday != -1:                                                    #There is no filter present for weekday
        data_filtered = data_all[(data_all['city_name'] == city) & (data_all['weekday'] == weekday)]
    else:                                                                                               #Every filter applied
        data_filtered = data_all[(data_all['city_name'] == city) & (data_all['month'] == month) & (data_all['weekday'] == weekday)]
    
    time_spent = round(timeit.default_timer() - time_start, 3)
    print('Bikeshare data filtered based on input criteria. This took: ' + str(time_spent) + ' seconds.')
    
    return data_filtered


def display_outputs(data, city, month, weekday):

    print('Calculating based on selected criteria.......')
    time_start = timeit.default_timer()
    
    # TO DO: display the most common month
    if month == 0:
        data_grouped = data.groupby(['month_name']).size().sort_values(ascending=False).reset_index(name='month_count')
        printout = data_grouped[['month_name','month_count']][data_grouped['month_count'] == data_grouped['month_count'].max()]
        
        print('\nThe most common month:\n' + str(printout))
    
    # TO DO: display the most common day of week
    if weekday == -1:
        data_grouped = data.groupby(['weekday_name']).size().sort_values(ascending=False).reset_index(name='weekday_count')
        printout = data_grouped[['weekday_name','weekday_count']][data_grouped['weekday_count'] == data_grouped['weekday_count'].max()]
        print('\nThe most common day of week:\n' + str(printout))
    
    # TO DO: display the most common start hour
    data_grouped = data.groupby(['hour']).size().sort_values(ascending=False).reset_index(name='hour_count')
    printout = data_grouped[['hour','hour_count']][data_grouped['hour_count'] == data_grouped['hour_count'].max()]
    print('\nThe most common start hour:\n' + str(printout))
    
    # TO DO: display most commonly used start station
    data_grouped = data.groupby(['Start Station']).size().sort_values(ascending=False).reset_index(name='count')
    printout = data_grouped[['Start Station','count']][data_grouped['count'] == data_grouped['count'].max()]
    print('\nMost commonly used Start station:\n' + str(printout))
    
    # TO DO: display most commonly used end station
    data_grouped = data.groupby(['End Station']).size().sort_values(ascending=False).reset_index(name='count')
    printout = data_grouped[['End Station','count']][data_grouped['count'] == data_grouped['count'].max()]
    print('\nMost commonly used End station:\n' + str(printout))
    
    # TO DO: display most frequent combination of start station and end station trip
    data_grouped = data.groupby(['Start Station', 'End Station']).size().sort_values(ascending=False).reset_index(name='count')
    printout = data_grouped[['Start Station', 'End Station','count']][data_grouped['count'] == data_grouped['count'].max()]
    print('\nMost frequent combination of start station and end station trip:\n' + str(printout))
    
    # TO DO: display total travel time
    # TO DO: display mean travel time
    trip_duration = np.array(data['Trip Duration'].tolist())
    travel_sum = sum(trip_duration)
    travel_mean = trip_duration.mean()
    print('\nTotal travel time:  ' + str(travel_sum) + ' hours.')
    print('\nMean travel time:   ' + str(travel_mean) + ' hours.')
    
    # TO DO: Display counts of user types
    printout = data.groupby(['User Type']).size().sort_values(ascending=False).dropna().reset_index(name='count')
    print('\nCount of user types:\n' + str(printout))
    
    # TO DO: Display counts of gender
    gender_list = data['Gender'].dropna().tolist()
    if len(gender_list) == 0:
        print('\nCounts of gender: <No Gender data under existing filters.>')
    else:
        printout = data.groupby(['Gender']).size().sort_values(ascending=False).dropna().reset_index(name='count')
        print('\nCounts of gender:\n' + str(printout))
    
    # TO DO: Display earliest, most recent, and most common year of birth
    birth_year_df = data['Birth Year'].dropna()
    birth_year_list = data['Birth Year'].dropna().tolist()
    if len(birth_year_list) == 0:
        oldest = youngest = most_common = ' <No Birth Year data under existing filters.>'
    else:
        birth_year_arr = np.array(birth_year_list)
        most_common = birth_year_df.mode()[0]
        oldest = min(birth_year_arr)
        youngest = max(birth_year_arr)  
    
    print ('\nEarliest year of birth:    {}\nLatest year of birth:      {}\nMost common year of birth: {}'.format(oldest, youngest, most_common))

    time_spent = round(timeit.default_timer() - time_start, 3)
    print('\nOutputs were created in: ' + str(time_spent) + ' seconds.\n')
    print('-'*40)

    
def main():
    
    #Read current folder, take all CSVs inside and merge them into one DataFrame
    data_bikeshares = load_and_merge_bikeshare_data()
    #print(data_bikeshares) #used when troubleshooting

    while True:
        #Get inputs:
        while True:
            city, month, weekday = get_filters()
            restart = input('Do you wish to proceed with these filters on? Enter yes or no.\n')
            if restart.lower() not in ('no', 'n'):
                break
        #Fixed inputs used when troubleshooting
        #city, month, weekday = 0,0,-1
        
        #Apply filters to merged data
        bikeshares_filtered = apply_filters(data_bikeshares, city, month, weekday)       
        #print(bikeshares_filtered)  #used when troubleshooting
        
        #Return values for filtered data
        display_outputs(bikeshares_filtered, city, month, weekday)
        
        #Restart
        restart = input('\nWould you like to restart? Press "yes" or "y" for restart, any other key for exit.\n')
        if restart.lower() not in ('yes', 'y'):
            print('Thank you for using this program!')
            break


if __name__ == '__main__':
	main()