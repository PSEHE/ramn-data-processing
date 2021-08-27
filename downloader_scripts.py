import requests
import datetime
import pandas as pd
import json
import os



### FUNCTIONS TO LOGIN
def create_aqc_login(username, password):
    
    credentials = {'UserName': username, 'Password': password}
    
    aqc_login = requests.post('https://cloud.aeroqual.com/api/account/login/', data = credentials)
    
    print('Login status:', aqc_login)
    
    return(aqc_login)

def get_instruments_available(aqc_login):
    
    available_aqys = requests.get('https://cloud.aeroqual.com/api/instrument', cookies = aqc_login.cookies)
    
    print('Instrument list request status:', available_aqys)
    
    return(available_aqys.json())



### FUNCTIONS TO FIGURE OUT WHEN TO START REQUEST
def get_deployment_date(aqy_id):
    
    deployment_network = pd.read_csv('monitor_deployment.txt', sep = '\t')
    deployment_aqy = deployment_network[deployment_network['Monitor ID'] == aqy_id]
    
    deployment_date = deployment_aqy.iloc[0]['Deployment date']
    deployment_split = str.split(deployment_date, '-')
    
    deployment_date_dt = datetime.date(int(deployment_split[0]), int(deployment_split[1]), int(deployment_split[2]))
    
    return(deployment_date_dt)

def get_colocation_date(aqy_id):

    colocation_network = pd.read_csv('monitor_deployment.txt', sep = '\t')
    colocation_aqy = colocation_network[colocation_network['Monitor ID'] == aqy_id]
    
    colocation_date = colocation_aqy.iloc[0]['Sacramento date']
    colocation_split = str.split(colocation_date, '-')
    
    colocation_date_dt = datetime.date(int(colocation_split[0]), int(colocation_split[1]), int(colocation_split[2]))
    
    return(colocation_date_dt)

def get_colocation_end(aqy_id):

    colocation_network = pd.read_csv('monitor_deployment.txt', sep = '\t')
    colocation_aqy = colocation_network[colocation_network['Monitor ID'] == aqy_id]
    
    colocation_end = colocation_aqy.iloc[0]['Sacramento end']
    colocation_split = str.split(colocation_end, '-')
    
    colocation_end_dt = datetime.date(int(colocation_split[0]), int(colocation_split[1]), int(colocation_split[2]))
    
    return(colocation_end_dt)

def get_existing_dates_in_data(existing_file_path):
    
    existing_data = pd.read_csv(existing_file_path)
    existing_data['date_observed'] = pd.to_datetime(existing_data['Time'].str[0:11])
    
    existing_dates = pd.DataFrame(set(existing_data['date_observed'])).rename(columns = {0: 'date_observed'}).sort_values('date_observed')
    existing_dates['time_dif'] = existing_dates['date_observed'].diff()
    existing_dates = existing_dates.dropna()
        
    return(existing_dates)  

def find_first_data_gap(existing_dates):
    
    dates_observed = existing_dates['date_observed']
    dates_observed_dt = set([datetime.date(date_observed.year, date_observed.month, date_observed.day) for date_observed in dates_observed])

    first_date_observed = existing_dates['date_observed'][0]
    eight_weeks_ago = pd.Timestamp.today() - pd.Timedelta(value = 56, unit = 'D')

    yesterday = pd.Timestamp.today() - pd.Timedelta(value = 1, unit = 'D')

    dates_possible = pd.date_range(max(first_date_observed, eight_weeks_ago), yesterday)
    
    dates_missing = set(dates_possible.date)
    dates_missing.difference_update(dates_observed_dt)
    
    if len(dates_missing) >= 1:
        
        first_missing_date = min(dates_missing)
    
        return(first_missing_date)
    else:
        
        return(None)

def check_existing_get_start_date(aqy_id, sacramento):

    existing_min_data_aqy = os.path.join('raw_minute_' + aqy_id + '.csv')
    existing_files_all_aqys = os.listdir('results_downloader_data/one_minute_data')
    
    deployment_date = get_deployment_date(aqy_id)
    
    if sacramento == True:

        colocation_date = get_colocation_date(aqy_id)

        return(colocation_date)
    
    else:
        if existing_min_data_aqy in existing_files_all_aqys:  
            
            existing_dates = get_existing_dates_in_data(os.path.join('results_downloader_data/one_minute_data', existing_min_data_aqy))
            first_data_gap = find_first_data_gap(existing_dates)                                         
            
            if first_data_gap is None:
                
                first_day_needed = 'None - up to date'
                
            else:
                
                first_day_needed = max(deployment_date, first_data_gap)
            
            return(first_day_needed)
             
        else:
            
            return(deployment_date)

def get_end_date(aqy_id, sacramento):

    if sacramento == True:

        start_date = get_colocation_date(aqy_id)

        end_date = get_colocation_end(aqy_id)

    else:
        end_date = datetime.date.today() - datetime.timedelta(days = 1)

    return(end_date)


def get_all_query_dates(start_date):
    
    yesterday = datetime.date.today() - datetime.timedelta(days = 1)
    
    desired_dates = {'start_date_dt': start_date, 
                     'end_date_dt': None, 
                     'start_date_str': None,
                     'end_date_str': None
                    }
    
    desired_dates['end_date_dt'] = min(desired_dates['start_date_dt'] + datetime.timedelta(days = 6), yesterday)
    
    desired_dates['start_date_str'] = desired_dates['start_date_dt'].strftime('%Y-%m-%d')
    desired_dates['end_date_str'] = desired_dates['end_date_dt'].strftime('%Y-%m-%d')
    
    return(desired_dates)



### FUNCTIONS TO SET UP INDIVIDUAL REQUESTS
def create_request_url(aqy_id, start_date, end_date):
    
    request_url_base = 'https://api.cloud.aeroqual.com/V2/'
    request_url_project = 'organisations/PSE%20Healthy%20Energy/projects/richmond_air/data?'
    request_url_id = 'instruments/{}/data?'.format(str(aqy_id.replace(' ', '%20')))
    request_url_dates = 'averagingPeriod=1&from={}%2000%3A00%3A00&to={}%2023%3A59%3A59&includeDiagnostics=true&rawValues=false&utc=false&includeSensorsWithNoData=true'.format(start_date, end_date)
    
    if aqy_id == 'Project':
        request_url = request_url_base + request_url_project + request_url_dates
    else:
        request_url = request_url_base + request_url_id + request_url_dates
    
    return(request_url)

def get_aqy_data_for_week(aqc_login, request_url): 
    
    aqy_data = requests.get(request_url, cookies = aqc_login.cookies)
    request_status = str(aqy_data)[-5:-2]
    
    if request_status != '200':
        print('Request status:', aqy_data)
        return(aqy_data.content)
    
    aqy_data_json = aqy_data.json()
    
    if 'Data' in aqy_data_json['Instruments'][0].keys():
        aqy_data_df = pd.json_normalize(aqy_data_json['Instruments'][0]['Data'])
    
        return(aqy_data_df)
    
    else:
        return(pd.DataFrame())

def initialize_results(aqy_id):
    
    existing_results_aqy = 'raw_minute_' + aqy_id + '.csv'
    existing_results_folder = 'results_downloader_data/one_minute_data'
    
    if existing_results_aqy in os.listdir(existing_results_folder):
        existing_data_df = pd.read_csv(os.path.join(existing_results_folder, existing_results_aqy))
        
        return(existing_data_df)
    
    else:
        blank_df = pd.DataFrame()
        
        return(blank_df)



### FUNCTION TO REQUEST FULL DEPLOYMENT FOR ONE AQY
def get_aqy_data_for_full_deployment(aqc_login, aqy_id, sacramento):
    
    date_to_start = check_existing_get_start_date(aqy_id, sacramento)
    date_to_stop = get_end_date(aqy_id, sacramento)

    if date_to_start == 'None - up to date': 
        return(print('Moving on to next AQY - data already up to date'))
    else:
        query_dates = get_all_query_dates(date_to_start)
    
    aqy_data_full = initialize_results(aqy_id)
    
    while query_dates['start_date_dt'] < date_to_stop:
        print(query_dates['start_date_dt'], query_dates['end_date_dt'])
        
        request_url = create_request_url(aqy_id, query_dates['start_date_str'], query_dates['end_date_str'])
        aqy_data = get_aqy_data_for_week(aqc_login, request_url)
    
        query_dates = get_all_query_dates(query_dates['start_date_dt'] + datetime.timedelta(days = 7))
        
        if aqy_data.shape[1] > 1:
            aqy_data_full = pd.concat([aqy_data_full, aqy_data])
    
    aqy_data_full['ID'] = aqy_id
    aqy_data_full.drop_duplicates(inplace = True)
    
    print('Data downloaded from cloud and appended to existing file')
    
    if sacramento == True:
        path_to_new_data = 'results_downloader_data/sac_colocation/sac_raw_minute_{}.csv'.format(aqy_id)

    else:
        path_to_new_data = 'results_downloader_data/one_minute_data/raw_minute_{}.csv'.format(aqy_id)

    aqy_data_full.to_csv(path_to_new_data, index = False)
    
    return(aqy_data_full)

