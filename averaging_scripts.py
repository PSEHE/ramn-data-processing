import pandas as pd
import os


### PROCESS AND CLEAN MINUTE DATA
def read_and_concat_min_data(sacramento=False):

	if sacramento == True:
		min_directory = 'results_downloader_data/sac_colocation'

	else:
		min_directory = 'results_downloader_data/one_minute_data'

	min_files = os.listdir(min_directory)

	min_data_network = pd.DataFrame()

	for min_file in min_files:

		#print('Reading data for', min_file)

		min_file_path = os.path.join(min_directory, min_file)
		min_data_aqy = pd.read_csv(min_file_path)
		min_data_network = pd.concat([min_data_network, min_data_aqy])

	min_data_network.reset_index(inplace = True)
	min_data_network.drop(columns = 'index', inplace = True)

	print('Concatenated all minute data files')

	return(min_data_network)


def clean_concatenated_data(minute_network_raw):

	minute_network_raw.columns = minute_network_raw.columns.str.replace('Data.', '')
	
	minute_network_raw['Time'] = minute_network_raw['Time'].str.replace('T', ' ')
	minute_network_raw['Ox'] = minute_network_raw['NO2'] + 1.1*minute_network_raw['O3']

	minute_network_raw = minute_network_raw[['ID', 'Time', 'TEMP', 'RH', 'DP', 'O3', 'NO2', 'Ox', 'PM2.5', 'PM2.5 raw']]

	return(minute_network_raw)



### CHECK IF ENOUGH AND TAKE AVERAGE
def average_minute_data(minute_network, averaging_time):
	timestamp_cutoffs_dict = {'sixty':-6, 'ten':-4}
	minutes_expected_dict = {'sixty':60, 'ten':10}
	minutes_fill_dict = {'sixty':':00:00', 'ten':'0:00'}
	
	timestamp_cutoff = timestamp_cutoffs_dict[averaging_time]
	minutes_expected = minutes_expected_dict[averaging_time]
	minutes_fill = minutes_fill_dict[averaging_time]

	minute_network['time'] = minute_network.loc['Time'].str[0:timestamp_cutoff]
	minute_network['minutes_observed'] = minute_network.groupby(['ID', 'time'])['time'].transform('count')
	minute_network['pct_complete'] = minute_network['minutes_observed']/minutes_expected
	minute_network = minute_network[minute_network['pct_complete'] >= 0.75]
	
	avg_network = minute_network.groupby(['ID', 'time']).mean().round(1).reset_index()
	avg_network['TIME'] = avg_network['time'] + minutes_fill
	avg_network = avg_network[['ID', 'TIME', 'TEMP', 'RH', 'DP', 'O3', 'Ox', 'NO2', 'PM2.5', 'PM2.5 raw']]
	avg_network = avg_network.rename(columns = {'TIME':'Time', 'PM2.5':'PM25', 'PM2.5 raw':'PM25_raw'})

	return(avg_network)