import os
import pandas as pd
from datetime import datetime, timedelta
from calendar import month_abbr


base_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))

def str_to_bool(val, default=False):
    if isinstance(val, str) and (val in os.environ):
        return True if os.environ[val] == 'True' else False
    else:
        return default

# Scenario setup
location = 'NY'
multi_home_sim = False # whether doing cosim on more than 1 home
if not multi_home_sim:     
    house_number = 1
    house_number_list = None
    scenario_name = location + '_house_' + str(house_number)
else:
    house_number = None
    house_number_list = [1]
    scenario_name = location + '_multi_home_sim'


# COsim date range, and timestep
year = 2007
month = "Aug" # enumeration Jan, May, or Aug
month_num = list(month_abbr).index(month)
start_date = 25
start_time = datetime(year, month_num, start_date, 0, 0)  # (Year, Month, Day, Hour, Min)
duration = timedelta(days=7)
time_step = timedelta(minutes = 1)
end_time = start_time + duration
sim_times = pd.date_range(start=start_time, end=end_time, freq=time_step)[:-1] # timestamps of cosim


# What Agents are included? Agents for ochre house model and hems
include_house = True
include_hems = True

# Frequency of Updates
freq_house = timedelta(minutes=1)  
freq_hems = timedelta(minutes=15)  #HEMS model aggregates data to this timestep. Should be greater than house frequency
freq_save_results = timedelta(minutes=5) 

# Time offsets for communication order
offset_house_run = timedelta(seconds=0)
offset_hems_to_house = timedelta(seconds=60)
offset_house_to_hems = timedelta(seconds=60)
offset_save_results = timedelta(0)

# House Variables
house_battery_schedule =  'TOU'

# HEMS Variables
hems_scenario = 'foresee'
hems_horizon = timedelta(hours=12)  # 12 hours / 15 min frequency = 180 time steps

# Input/Output file paths
input_path = os.path.join(base_path, "inputs")
output_path = os.path.join(base_path, "outputs")
baseline_output_path = os.path.join(base_path, "outputs", location)

ochre_input_path = os.path.join(input_path, "ochre")
foresee_input_path = os.path.join(input_path, "foresee")


house_args_file_name = location + '_houses.xlsx'
house_args_file = os.path.join(input_path, 'house_args', house_args_file_name)

# Output file locations
house_results_path = os.path.join(output_path, location, 'ochre')
house_results_file = os.path.join(house_results_path, '{}_out.csv')
hems_results_path = os.path.join(output_path, location, 'foresee')

# Load house args spread sheed
df_house_args = pd.read_excel(house_args_file, skiprows= 1, index_col='Bldg_ID')
if not multi_home_sim:
    house_id = 'house_' + str(house_number)
in_real_time = str_to_bool(['REAL_TIME'])
debug = False

#store all sim parameters in dictionary
dict_sim_params = {
    'base_path': base_path,
    'in_real_time': in_real_time,
    'debug': debug,
    'location': location,
    'multi_home_sim': multi_home_sim,
    'house_number': house_number,
    'house_number_list': house_number_list,
    'scenario_name': scenario_name,
    'month': month,
    'start_time': start_time,
    'end_time': end_time,
    'duration': duration,
    'time_step': time_step,
    'sim_times': sim_times,
    'include_house': include_house,
    'include_hems': include_hems,
    'freq_house': freq_house,
    'freq_hems': freq_hems,
    'freq_save_results': freq_save_results,
    'offset_house_run': offset_house_run,
    'offset_hems_to_house': offset_hems_to_house,
    'offset_house_to_hems': offset_house_to_hems,
    'offset_save_results': offset_save_results,
    'house_battery_schedule': house_battery_schedule,
    'hems_scenario': hems_scenario,
    'hems_horizon': hems_horizon,
    'input_path': input_path,
    'output_path': output_path,
    'baseline_output_path': baseline_output_path,
    'ochre_input_path': ochre_input_path,
    'foresee_input_path': foresee_input_path,
    'house_results_path': house_results_path,
    'house_results_file': house_results_file,
    'hems_results_path': hems_results_path,
    'df_house_args': df_house_args,
}