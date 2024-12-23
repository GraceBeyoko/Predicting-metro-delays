import requests
import pandas as pd
from datetime import datetime
import os

stops_data = pd.read_csv('./Data/arrets.csv', sep=';')
stops_data = stops_data[stops_data['ArRType'].isin(['metro', 'rail'])]
stops_data = stops_data.sort_values(by=['ArRType', 'ArRId'])
stops_data = stops_data[['ArRId', 'ArRName', 'ArRType', 'ArRTown']]

df_lines = pd.read_csv('./Data/referentiel-des-lignes.csv', sep=';')

line_refs = df_lines[(~df_lines['TransportSubmode'].isin(['suburbanRailway', 'regionalRail', 'railShuttle']))]
line_refs = line_refs[['ID_Line', 'TransportMode', 'Name_Line']]
line_refs = line_refs.sort_values(by='ID_Line')


base_url = 'https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring'
headers = {
    'Accept': 'application/json',
    'apikey': 'JPEmiEtsjEBKTAlN9VdADYgYSzyWIfCy'
}

# Function to calculate time difference in minutes
def calculate_time_difference(aimed_time, expected_time):
    if aimed_time != 'N/A' and expected_time != 'N/A':
        aimed_dt = pd.to_datetime(aimed_time)
        expected_dt = pd.to_datetime(expected_time)
        diff_seconds = (expected_dt - aimed_dt).total_seconds()
        return round(diff_seconds / 60, 2)
    return None

# Function to fetch and log data for a stop reference
def log_stop_data(stop_reference, transport_mode):
    params = {
        'MonitoringRef': f'STIF:StopPoint:Q:{stop_reference}:',
        'LineRef': ''
    }

    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code == 200:
        try:
            data = response.json()

            if 'Siri' in data and 'ServiceDelivery' in data['Siri'] and 'StopMonitoringDelivery' in data['Siri']['ServiceDelivery']:
                stop_monitoring = data['Siri']['ServiceDelivery']['StopMonitoringDelivery'][0]['MonitoredStopVisit']

                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                train_list = []

                for stop in stop_monitoring:
                    vehicle_journey = stop.get('MonitoredVehicleJourney', {})
                    monitored_call = vehicle_journey.get('MonitoredCall', {})

                    line_ref = vehicle_journey.get('LineRef', {}).get('value', 'N/A')
                    destination_name = vehicle_journey.get('DestinationName', [{}])[0].get('value', 'N/A') if vehicle_journey.get('DestinationName') else 'N/A'
                    stop_name = monitored_call.get('StopPointName', [{}])[0].get('value', 'N/A') if monitored_call.get('StopPointName') else 'N/A'

                    departure_status = monitored_call.get('DepartureStatus', 'N/A')
                    aimed_arrival = monitored_call.get('AimedArrivalTime', 'N/A')
                    expected_arrival = monitored_call.get('ExpectedArrivalTime', 'N/A')
                    aimed_departure = monitored_call.get('AimedDepartureTime', 'N/A')
                    expected_departure = monitored_call.get('ExpectedDepartureTime', 'N/A')
                    recorded_at_time = stop.get('RecordedAtTime', 'N/A')

                    arrival_diff = calculate_time_difference(aimed_arrival, expected_arrival)
                    departure_diff = calculate_time_difference(aimed_departure, expected_departure)

                    line_ref1 = line_ref.replace('STIF:Line::', '').rstrip(':')

                    if line_ref1 in line_refs['ID_Line'].values:
                        if departure_status in ['cancelled', 'delayed'] or (arrival_diff and arrival_diff >= 5) or (departure_diff and departure_diff >= 5):
                            train_list.append({
                                'timestamp': timestamp,
                                'stop_reference': stop_reference,
                                'stop_name': stop_name,
                                'line_ref': line_ref,
                                'transport_mode': transport_mode,
                                'destination_name': destination_name,
                                'departure_status': departure_status,
                                'scheduled_arrival': aimed_arrival,
                                'real_arrival': expected_arrival,
                                'scheduled_departure': aimed_departure,
                                'real_departure': expected_departure,
                                'arrival_difference': arrival_diff,
                                'departure_difference': departure_diff,
                                'recorded_at_time': recorded_at_time
                            })

                if train_list:
                    df_trains = pd.DataFrame(train_list)

                    metro_path = './metro_delays.csv'
                    rail_path = './rail_delays.csv'

                    if transport_mode == 'metro':
                        path = metro_path
                    elif transport_mode == 'rail':
                        path = rail_path
                    else:
                        print(f"Unknown transport mode: {transport_mode}")
                        return

                    path_exists = os.path.isfile(path)

                    df_trains.to_csv(path, mode='a', sep=',', header=not path_exists, index=False, encoding='utf-8')

            else:
                print(f"'StopMonitoringDelivery' key not found in the data for MonitoringRef {stop_reference}. Check the response structure.")

        except requests.JSONDecodeError:
            print('Failed to decode JSON for MonitoringRef', stop_reference)
    else:
        print(f'Error for MonitoringRef {stop_reference}: HTTP {response.status_code}')
        print('Response Content:', response.text)


def stop_loop():
    for _, row in stops_data.iterrows():
        stop_ref = row['ArRId']
        transport_mode = row['ArRType']
        log_stop_data(stop_ref, transport_mode)

stop_loop()


def log_stop_data_onTime(stop_reference, transport_mode):
    params = {
        'MonitoringRef': f'STIF:StopPoint:Q:{stop_reference}:',
        'LineRef': ''
    }

    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code == 200:
        try:
            data = response.json()

            if 'Siri' in data and 'ServiceDelivery' in data['Siri'] and 'StopMonitoringDelivery' in data['Siri']['ServiceDelivery']:
                stop_monitoring = data['Siri']['ServiceDelivery']['StopMonitoringDelivery'][0]['MonitoredStopVisit']

                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                train_list = []

                for stop in stop_monitoring:
                    vehicle_journey = stop.get('MonitoredVehicleJourney', {})
                    monitored_call = vehicle_journey.get('MonitoredCall', {})

                    line_ref = vehicle_journey.get('LineRef', {}).get('value', 'N/A')
                    destination_name = vehicle_journey.get('DestinationName', [{}])[0].get('value', 'N/A') if vehicle_journey.get('DestinationName') else 'N/A'
                    stop_name = monitored_call.get('StopPointName', [{}])[0].get('value', 'N/A') if monitored_call.get('StopPointName') else 'N/A'

                    departure_status = monitored_call.get('DepartureStatus', 'N/A')
                    aimed_arrival = monitored_call.get('AimedArrivalTime', 'N/A')
                    expected_arrival = monitored_call.get('ExpectedArrivalTime', 'N/A')
                    aimed_departure = monitored_call.get('AimedDepartureTime', 'N/A')
                    expected_departure = monitored_call.get('ExpectedDepartureTime', 'N/A')
                    recorded_at_time = stop.get('RecordedAtTime', 'N/A')

                    arrival_diff = calculate_time_difference(aimed_arrival, expected_arrival)
                    departure_diff = calculate_time_difference(aimed_departure, expected_departure)

                    line_ref1 = line_ref.replace('STIF:Line::', '').rstrip(':')

                    if line_ref1 in line_refs['ID_Line'].values:
                        if departure_status in ['onTime'] or (arrival_diff and arrival_diff < 5) or (departure_diff and departure_diff < 5):
                            train_list.append({
                                'timestamp': timestamp,
                                'stop_reference': stop_reference,
                                'stop_name': stop_name,
                                'line_ref': line_ref,
                                'transport_mode': transport_mode,
                                'destination_name': destination_name,
                                'departure_status': departure_status,
                                'scheduled_arrival': aimed_arrival,
                                'real_arrival': expected_arrival,
                                'scheduled_departure': aimed_departure,
                                'real_departure': expected_departure,
                                'arrival_difference': arrival_diff,
                                'departure_difference': departure_diff,
                                'recorded_at_time': recorded_at_time
                            })

                if train_list:
                    df_trains = pd.DataFrame(train_list)

                    metro_path = './metro_onTime.csv'
                    rail_path = './rail_onTime.csv'

                    if transport_mode == 'metro':
                        path = metro_path
                    elif transport_mode == 'rail':
                        path = rail_path
                    else:
                        print(f"Unknown transport mode: {transport_mode}")
                        return

                    path_exists = os.path.isfile(path)

                    df_trains.to_csv(path, mode='a', sep=',', header=not path_exists, index=False, encoding='utf-8')

            else:
                print(f"'StopMonitoringDelivery' key not found in the data for MonitoringRef {stop_reference}. Check the response structure.")

        except requests.JSONDecodeError:
            print('Failed to decode JSON for MonitoringRef', stop_reference)
    else:
        print(f'Error for MonitoringRef {stop_reference}: HTTP {response.status_code}')
        print('Response Content:', response.text)


def stop_loop_timely():
    for _, row in stops_data.iterrows():
        stop_ref = row['ArRId']
        transport_mode = row['ArRType']
        log_stop_data_onTime(stop_ref, transport_mode)

stop_loop_timely()