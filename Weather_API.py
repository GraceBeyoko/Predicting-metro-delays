#pip install openmeteo-requests
#pip install requests-cache retry-requests numpy pandas

import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
params = {
	"latitude": 48.8534,
	"longitude": 2.3488,
	"start_date": "2024-09-24",
	"end_date": "2024-11-27",
	"minutely_15": ["temperature_2m", "precipitation", "rain", "snowfall", "wind_speed_10m", "wind_gusts_10m", "visibility", "is_day"],
	"hourly": ["temperature_2m", "precipitation", "visibility", "wind_speed_10m"]
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Process minutely_15 data. The order of variables needs to be the same as requested.
minutely_15 = response.Minutely15()
minutely_15_temperature_2m = minutely_15.Variables(0).ValuesAsNumpy()
minutely_15_precipitation = minutely_15.Variables(1).ValuesAsNumpy()
minutely_15_rain = minutely_15.Variables(2).ValuesAsNumpy()
minutely_15_snowfall = minutely_15.Variables(3).ValuesAsNumpy()
minutely_15_wind_speed_10m = minutely_15.Variables(4).ValuesAsNumpy()
minutely_15_wind_gusts_10m = minutely_15.Variables(5).ValuesAsNumpy()
minutely_15_visibility = minutely_15.Variables(6).ValuesAsNumpy()
minutely_15_is_day = minutely_15.Variables(7).ValuesAsNumpy()

minutely_15_data = {"date": pd.date_range(
	start = pd.to_datetime(minutely_15.Time(), unit = "s", utc = True),
	end = pd.to_datetime(minutely_15.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = minutely_15.Interval()),
	inclusive = "left"
)}
minutely_15_data["temperature_2m"] = minutely_15_temperature_2m
minutely_15_data["precipitation"] = minutely_15_precipitation
minutely_15_data["rain"] = minutely_15_rain
minutely_15_data["snowfall"] = minutely_15_snowfall
minutely_15_data["wind_speed_10m"] = minutely_15_wind_speed_10m
minutely_15_data["wind_gusts_10m"] = minutely_15_wind_gusts_10m
minutely_15_data["visibility"] = minutely_15_visibility
minutely_15_data["is_day"] = minutely_15_is_day

minutely_15_dataframe = pd.DataFrame(data = minutely_15_data)
print(minutely_15_dataframe)

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
hourly_visibility = hourly.Variables(2).ValuesAsNumpy()
hourly_wind_speed_10m = hourly.Variables(3).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}
hourly_data["temperature_2m"] = hourly_temperature_2m
hourly_data["precipitation"] = hourly_precipitation
hourly_data["visibility"] = hourly_visibility
hourly_data["wind_speed_10m"] = hourly_wind_speed_10m

hourly_dataframe = pd.DataFrame(data = hourly_data)
print(hourly_dataframe)
