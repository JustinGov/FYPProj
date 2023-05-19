import numpy as np
import pandas as pd
import requests
import time

# set WeatherAPI key and locations
api_key = '029a436baa02494fbe583505230304'
locations = ['Singapore', 'Australia', 'Malaysia']

def main():
    while True:
        for loc in locations:
            # create empty lists to store weather data
            last_updated = []
            temp_c = []
            precip_mm = []
            humidity = []
            cloud = []
            uv = []
            try:
                url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={loc}&field=last_updated,temp_c,precip_mm,humidity,cloud,uv"
                response = requests.get(url)
                response.raise_for_status()  # raise an exception for any errors
                data = response.json()
                last_updated.append(data['current']['last_updated'])
                temp_c.append(data['current']['temp_c'])
                precip_mm.append(data['current']['precip_mm'])
                humidity.append(data['current']['humidity'])
                cloud.append(data['current']['cloud'])
                uv.append(data['current']['uv'])

                # create dataframe to store weather data
                weather_df = pd.DataFrame({
                    'Location': [loc],
                    'Last Updated': last_updated,
                    'Temperature (C)': temp_c,
                    'Precipitation (mm)': precip_mm,
                    'Humidity': humidity,
                    'Cloud Cover': cloud,
                    'UV Index': uv
                })
                # store data in a CSV file
                weather_df.to_csv('Kafka\data\data.csv', mode='a', header=False, index=False) #append data to csv file
                # print the dataframe
                #print(weather_df)
                
            except Exception as e:
                print("Error occurred: ", e)

            # wait for 60 seconds before sending the next batch of data
            time.sleep(60)

if __name__ == "__main__":
    main()

