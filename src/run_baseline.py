import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import xmltodict
import click
import pandas as pd
from datetime import datetime, timedelta
import shutil
from ochre import Dwelling
from constants import dict_sim_params

"""
Code for running simulations on OCHRE.

Author: Alex Lee (alexlee5124@gmail.com)
Date: 4/19/2025

"""


# Set up input arguments for dwelling
HOUSE_DEFAULT_ARGS = {
# Timing parameters
'start_time': dict_sim_params['start_time'],
'time_res': dict_sim_params['time_step'],
'duration': dict_sim_params['duration'],
'initialization_time': timedelta(days=7),
'ext_time_res': dict_sim_params['freq_hems'], #HEMS model aggregates data to this timestep. Should be greater than house frequency

# Output settings
'save_results': True,
'output_path': dict_sim_params['baseline_output_path'],
'verbosity': 9,  # verbosity of results file (0-9); 8: include envelope; 9: include water heater
'metrics_verbosity': 7,
}

# Function for extracting weather station name from an HPXML file
def extract_weather_station(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            buildingXML = xmltodict.parse(file.read())
            weather_station_name = buildingXML['HPXML']['Building']['BuildingDetails']['ClimateandRiskZones']['WeatherStation']['Name']
    except Exception as e:
        print(f"Error parsing XML {file_path}: {e}")

    return weather_station_name

def remove_directory(path):
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
        except Exception as e:
            print(f"Error deleting {path}: {e}")
    else:
        print(f"The filepath does not exist: {path}")

@click.command()
@click.argument('building_id', type=int)
@click.argument('upgrade_id', type=int)
def simulate_dwelling(
        building_id:int,
        upgrade_id:int
        ):
    """
    simulate: This function runs an OCHRE simulation for a building. It filters the building by the
    building id, upgrade id, state, year, and version of the building XML file. You need to have already
    downloaded the building XML file prior to running this function.

    Input simulation parameters, such as duration, start date, simulation timestep size, etc.

    Author: Alex Lee (alexlee5124@gmail.com)
    Date: 3/29/2025
    """

    # Update dwelling arguments
    house_id = f"bldg{building_id:07}-up{upgrade_id:02}"
    house_args = HOUSE_DEFAULT_ARGS.copy()
    house_row = dict_sim_params['df_house_args'].loc[house_id]
    house_args.update(house_row)
    house_args['name'] = f"bldg{building_id:07}-up{upgrade_id:02}"

    # Update input files
    # HPXML file
    house_args['hpxml_file'] = os.path.join(
        dict_sim_params['ochre_input_path'],
        house_args['hpxml_file']
    )
    if not os.path.exists(house_args['hpxml_file']):
        print(f"HPXML file does not exist: {house_args['hpxml_file']}")
        sys.exit(1)

    # Schedule file
    house_args['hpxml_schedule_file'] = os.path.join(
        dict_sim_params['ochre_input_path'],
        house_args['hpxml_schedule_file']
    )
    if not os.path.exists(house_args['hpxml_schedule_file']):
        print(f"Schedule file does not exist: {house_args['hpxml_schedule_file']}")
        sys.exit(1)
    # Weather File
    weather_station = f"{extract_weather_station(house_args['hpxml_file'])}.epw"
    house_args['weather_file'] = os.path.join(
        dict_sim_params['ochre_input_path'],
        weather_station
    )
    if not os.path.exists(house_args['weather_file']):
        print(f"Weather file does not exist: {house_args['weather_file']}")
        sys.exit(1)

    try:
        dwelling = Dwelling(**house_args)
        dwelling.simulate()
    except Exception as e:
        print(f"Simulation failed for bldg{building_id:07}-up{upgrade_id:02}: {e}")
    
if __name__ == '__main__':
    simulate_dwelling()