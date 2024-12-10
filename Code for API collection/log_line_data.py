import requests
import pandas as pd
from datetime import datetime
import os

base_url = 'https://prim.iledefrance-mobilites.fr/marketplace/general-message'

headers = {
    'Accept': 'application/json',
    'apikey': 'JPEmiEtsjEBKTAlN9VdADYgYSzyWIfCy'  # Ensure this is a valid key
}

df_lines = pd.read_csv('./Data/referentiel-des-lignes.csv', sep=';')

filtered_lines = df_lines[(df_lines['TransportMode'].isin(['metro', 'rail'])) & 
                          (~df_lines['TransportSubmode'].isin(['regionalRail', 'railShuttle']))]

line_refs = filtered_lines[['ID_Line', 'TransportMode', 'Name_Line']]
line_refs = line_refs.sort_values(by='ID_Line')



def log_data(line_ref, transport_mode, line_name):    
    params = {
        'LineRef': line_ref,
        'StopPointRef': '',  
        'InfoChannelRef': ''
    }
    
    response = requests.get(base_url, headers=headers, params=params)
            
    if response.status_code == 200: #successful response
        try:
            data = response.json()
    
            if 'Siri' in data and 'ServiceDelivery' in data['Siri'] and 'GeneralMessageDelivery' in data['Siri']['ServiceDelivery']:
                general_messages = data['Siri']['ServiceDelivery']['GeneralMessageDelivery'][0]['InfoMessage']
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
                perturbation_list = []
                
                for message in general_messages:
                    message_id = message.get('InfoMessageIdentifier', {}).get('value', 'N/A')
                    line_ref_value = message.get('Content', {}).get('LineRef', [{}])[0].get('value', 'N/A')
                    message_text = message.get('Content', {}).get('Message', [{}])[0].get('MessageText', {}).get('value', 'N/A')
                    recorded_at_time = message.get('RecordedAtTime', 'N/A')
                    valid_until_time = message.get('ValidUntilTime', 'N/A')
                    
                    perturbation_list.append({
                        'timestamp': timestamp,
                        'message_id': message_id,
                        'line': line_ref_value,
                        'line_name': line_name,
                        'transport_mode': transport_mode,
                        'message': message_text,
                        'recorded_at_time': recorded_at_time,
                        'valid_until_time': valid_until_time
                    })
                
                df_perturbations = pd.DataFrame(perturbation_list)

                path = './messages_log.csv'

                #check if the file exists (to handle headers appropriately)
                path_exists = os.path.isfile(path)

                #append data to the CSV
                df_perturbations.to_csv(path, mode='a', sep=',', header=not path_exists, index=False, encoding='utf-8')

            else:
                print(f"'GeneralMessageDelivery' key not found in the data for LineRef {line_ref}. Check the response structure.")
                
        except requests.JSONDecodeError:
            print('Failed to decode JSON for LineRef', line_ref)
    else:
        # If the request failed, print the error and response content
        print(f'Error for LineRef {line_ref} ({transport_mode} {line_name}):', response.status_code)
        print('Response Content:', response.text)




#loop through all unique LineRefs and call the log_data function for each one
def loop():
    for _, row in line_refs.iterrows():
        line_ref = f'STIF:Line::{row["ID_Line"]}:'
        transport_mode = row['TransportMode']
        line_name = row['Name_Line']
        log_data(line_ref, transport_mode, line_name)

loop()