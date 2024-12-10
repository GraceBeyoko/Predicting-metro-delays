import requests
import pandas as pd
from datetime import datetime
import os

def log_data_rer():
    base_url = 'https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/line_reports/physical_modes/physical_mode%3ARapidTransit/line_reports?count=100?language=en-GB'
    headers = {
        'Accept': 'application/json',
        'apikey': 'JPEmiEtsjEBKTAlN9VdADYgYSzyWIfCy'
    }

    response = requests.get(base_url, headers=headers)
    print(f'Status Code:', response.status_code)

    if response.status_code == 200:
        try:
            data = response.json()
            if 'disruptions' in data:
                disruptions = data['disruptions']
                print(f"Found {len(disruptions)} messages")

                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                disruption_list = []

                for disruption in disruptions:
                    disruption_id = disruption.get('disruption_id', 'N/A')
                    line_ref = disruption.get('impacted_objects', [{}])[0].get('pt_object', {}).get('id', 'N/A')
                    line_name = disruption.get('impacted_objects', [{}])[0].get('pt_object', {}).get('name', 'N/A')
                    cause = disruption.get('cause', 'N/A')
                    tags = disruption.get('tags')
                    category = disruption.get('category', 'N/A')
                    severity = disruption.get('severity', {}).get('effect', 'N/A')
                    status = disruption.get('status', 'N/A')
                    updated_at = disruption.get('updated_at', 'N/A')
                    begin = disruption.get('application_periods', [{}])[0].get('begin', 'N/A')
                    end = disruption.get('application_periods', [{}])[0].get('end', 'N/A')

                    messages = disruption.get('messages', [])
                    for message in messages:
                        message_text = message.get('text', 'N/A')
                        channel_name = message.get('channel', {}).get('name', 'N/A')

                        if channel_name in ['titre', 'moteur']:
                            disruption_list.append({
                                'timestamp': timestamp,
                                'disruption_id': disruption_id,
                                'ref': line_ref,
                                'name': line_name,
                                'message_text': message_text,
                                'channel_name': channel_name,
                                'cause': cause,
                                'tags': tags,
                                'category': category,
                                'severity': severity,
                                'status': status,
                                'updated_at': updated_at,
                                'begin': begin,
                                'end': end
                            })

                if disruption_list:
                    df_disruptions = pd.DataFrame(disruption_list)

                    path = './rer_line_reports.csv'
                    path_exists = os.path.isfile(path)

                    df_disruptions.to_csv(path, mode='a', sep=',', header=not path_exists, index=False, encoding='utf-8')

                print(f"Data logged successfully")

            else:
                print(f"'GeneralMessageDelivery' key not found in the data for LineRef {line_ref}. Check the response structure.")

        except requests.JSONDecodeError:
            print('Failed to decode JSON for LineRef', line_ref)
    else:
        print(f'Error for LineRef {line_ref}:', response.status_code)
        print('Response Content:', response.text)

def log_data_metro():

    base_url = 'https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/line_reports/physical_modes/physical_mode%3AMetro/line_reports?count=100&language=en-GB'

    headers = {
        'Accept': 'application/json',
        'apikey': 'JPEmiEtsjEBKTAlN9VdADYgYSzyWIfCy'
    }

    response = requests.get(base_url, headers=headers)

    print(f'Status Code:', response.status_code)

    if response.status_code == 200:
        try:
            data = response.json()

            if 'disruptions' in data:
                disruptions = data['disruptions']

                print(f"Found {len(disruptions)} messages")

                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                disruption_list = []

                for disruption in disruptions:
                    disruption_id = disruption.get('disruption_id', 'N/A')
                    line_ref = disruption.get('impacted_objects', [{}])[0].get('pt_object', {}).get('id', 'N/A')
                    line_name = disruption.get('impacted_objects', [{}])[0].get('pt_object', {}).get('name', 'N/A')
                    cause = disruption.get('cause', 'N/A')
                    tags = disruption.get('tags')
                    category = disruption.get('category', 'N/A')
                    severity = disruption.get('severity', {}).get('effect', 'N/A')
                    status = disruption.get('status', 'N/A')
                    updated_at = disruption.get('updated_at', 'N/A')
                    begin = disruption.get('application_periods', [{}])[0].get('begin', 'N/A')
                    end = disruption.get('application_periods', [{}])[0].get('end', 'N/A')

                    messages = disruption.get('messages', [])
                    for message in messages:
                        message_text = message.get('text', 'N/A')
                        channel_name = message.get('channel', {}).get('name', 'N/A')

                        if channel_name in ['titre', 'moteur']:
                            disruption_list.append({
                                'timestamp': timestamp,
                                'disruption_id': disruption_id,
                                'ref': line_ref,
                                'name': line_name,
                                'message_text': message_text,
                                'channel_name': channel_name,
                                'cause': cause,
                                'tags': tags,
                                'category': category,
                                'severity': severity,
                                'status': status,
                                'updated_at': updated_at,
                                'begin': begin,
                                'end': end
                            })

                if disruption_list:
                    df_disruptions = pd.DataFrame(disruption_list)

                    path = './metro_line_reports.csv'

                    path_exists = os.path.isfile(path)

                    df_disruptions.to_csv(path, mode='a', sep=',', header=not path_exists, index=False, encoding='utf-8')

                print(f"Data logged successfully")

            else:
                print(f"'GeneralMessageDelivery' key not found in the data for LineRef {line_ref}. Check the response structure.")

        except requests.JSONDecodeError:
            print('Failed to decode JSON for LineRef', line_ref)
    else:
        print(f'Error for LineRef {line_ref} ({transport_mode} {line_name}):', response.status_code)
        print('Response Content:', response.text)



log_data_rer()
log_data_metro()
