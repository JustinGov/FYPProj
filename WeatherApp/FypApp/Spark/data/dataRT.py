import numpy as np
import pandas as pd
import requests
import time
import argparse

# set WeatherAPI key and locations
api_key = '029a436baa02494fbe583505230304'

def main(locations):
    
    #weather_df.to_csv('data.csv', mode='a', header=True, index=False) #append data to csv file   
    
   
    while True:
        weather_df = pd.DataFrame(columns=['Location', 'Last Updated', 'Temperature (C)', 'Temperature (F)', 'Wind (km/hr)', 'Wind direction (in degree)', 'Wind direction (compass)', 'Pressure (millibars)','Precipitation (mm)', 'Humidity', 'Cloud Cover', 'UV Index', 'Wind gust (km/hr)'])
        for loc in locations:
            try:
                url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={loc}&field=last_updated,temp_c,temp_f,wind_kph,wind_degree,wind_dir,pressure_mb,precip_mm,humidity,cloud,uv,gust_kph"
                response = requests.get(url)
                response.raise_for_status()  # raise an exception for any errors
                data = response.json()
                last_updated =(data['current']['last_updated'])
                temp_c = (data['current']['temp_c'])
                temp_f = (data['current']['temp_f'])
                wind_kph = (data['current']['wind_kph'])
                wind_degree = (data['current']['wind_degree'])
                wind_dir = (data['current']['wind_dir'])
                pressure_mb = (data['current']['pressure_mb'])
                precip_mm = (data['current']['precip_mm'])
                humidity = (data['current']['humidity'])
                cloud = (data['current']['cloud'])
                uv = (data['current']['uv'])
                gust_kph = (data['current']['gust_kph'])

                # create dataframe to store weather data
                weather_df.loc[len(weather_df)] = ({
                    'Location': loc,
                    'Last Updated': last_updated,
                    'Temperature (C)': temp_c,
                    'Temperature (F)': temp_f,
                    'Wind (km/hr)': wind_kph,
                    'Wind direction (in degree)': wind_degree,
                    'Wind direction (compass)': wind_dir,
                    'Pressure (millibars)': pressure_mb,
                    'Precipitation (mm)': precip_mm,
                    'Humidity': humidity,
                    'Cloud Cover': cloud,
                    'UV Index': uv,
                    'Wind gust (km/hr)' : gust_kph
                })
                #weather_df.loc[len(weather_df)] = ['value1', 'value2', 'value3']
                # store data in a CSV file
                #weather_df.to_csv('data.csv', mode='w', header=False, index=False) #append data to csv file
                # print the dataframe
                #print(weather_df)
                
            except Exception as e:
                print("Error occurred: ", e)

        weather_df.to_csv('data.csv', mode='w', header=False, index=False)
        time.sleep(60)
         #append data to csv file    
            # wait for 60 seconds before sending the next batch of data
            

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("locations", nargs="+")
    args = parser.parse_args()
    main(args.locations)


