import argparse
import numpy as np
import pandas as pd
import requests
import time
from datetime import date
import json
import os
import datetime

# set WeatherAPI key
api_key = '05350069ed5b4c30ab1155034231705'

# make a directory to save the downloaded csv files
save_dir = "/app/datafiles"
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

def main(locations, Date_is, Date_end):
    # Convert the start and end dates to datetime objects
    start_date = datetime.datetime.strptime(Date_is, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(Date_end, "%Y-%m-%d")

    print(start_date)
    print(end_date)

    df_all=[] # make an empty dataframe
    for location in locations:
        while start_date <= end_date:
            try:
                # api call - data receive in json
                url = f"http://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={start_date}"
                print(url)
                response = requests.get(url)

                #check for any error
                response.raise_for_status()

                data = json.loads(response.text)

                # Create a DataFrame from the 'hour' list in the JSON data
                df = pd.DataFrame(data["forecast"]["forecastday"][0]["hour"])

                # Add new columns for location and time information
                df["location"] = location
                df["time"] = df["time"].str.split("+").str[0]

                # Parse the 'time' column to extract the hour information
                df["hour"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M").dt.hour

                # Set the 'time' and 'location' columns as the index
                df.set_index(["location", "time"], inplace=True)

                # Append the DataFrame to the list
                df_all.append(df)

                # Increment the current date by one day
                start_date += datetime.timedelta(days=1)

            except Exception as e:
                print("Error occur: ",e)
        start_date = datetime.datetime.strptime(Date_is, "%Y-%m-%d")  # reset start_date

    # Concatenate the DataFrames into a single DataFrame
    if df_all:
        concated_df = pd.concat(df_all)

    filename = 'historicalData_'
    filename = filename + ('_'.join(locations)) + '_' + Date_is + '.csv'
    # set up the path to save the csv files
    filePath = os.path.join(save_dir, filename)

    # Save the DataFrame to a CSV file
    concated_df.to_csv(filePath, index=True) # this is the previous filepath unchange

    #concated_df.to_csv("testing123456.csv", index=True) #this will save in window file 
    # when testing script use line 77 to save csv to window blank line 75

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("locations", nargs="+")
    parser.add_argument("Date_is")
    parser.add_argument("Date_end")
    args = parser.parse_args()
    main(args.locations, args.Date_is, args.Date_end)
