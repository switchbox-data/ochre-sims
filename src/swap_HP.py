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
import json
from ochre.utils import convert, OCHREException, nested_update, load_schedule, update_equipment_properties
from ochre.utils.hpxml import parse_hpxml_occupancy, parse_hpxml_envelope, parse_hpxml_equipment, import_hpxml, load_hpxml
from ochre.Models import Envelope
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
'initialization_time': timedelta(days=1),
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

def get_HP_info(HP_id:int):
    with open('inputs/equipments/HP.json', 'r') as file:
        HP_info = json.load(file)
    # Find the heat pump with matching id in the ASHP array
    for hp in HP_info['ASHP']:
        if hp['id'] == HP_id:
            return hp
    raise ValueError(f"No heat pump found with id {HP_id}")

def create_HP_equipment(HP_info:dict, hvac_type:str):
    name = HP_info['heat_pump_type']
    fuel = HP_info['fuel_type']
    capacity = HP_info[f'{hvac_type.lower()}_capacity']
    space_fraction = HP_info[f'fraction_{hvac_type.lower()}_load_served']
    efficiency = HP_info[f'annual_{hvac_type.lower()}_efficiency']
    efficiency_string = (f"{HP_info[f'annual_{hvac_type.lower()}_efficiency']} " 
                         f"{HP_info[f'annual_{hvac_type.lower()}_efficiency_units']}")
    if HP_info[f'annual_{hvac_type.lower()}_efficiency_units'] in ['Percent', 'AFUE']:
        eir = 1 / efficiency
        if HP_info[f'annual_{hvac_type.lower()}_efficiency_units'] == 'Percent':
            efficiency *= 100
    elif HP_info[f'annual_{hvac_type.lower()}_efficiency_units'] in ['EER', 'SEER', 'HSPF']:
        eir = 1 / convert(efficiency, 'Btu/hour', 'W')
    else:
        raise OCHREException(f'Unknown inputs for HVAC {hvac_type} efficiency: {efficiency}')
    
    # Get number of speeds
    speed_options = {
        'single stage': 1,
        'two stage': 2,
        'variable speed': 4,
    }
    if name == 'mini-split':
        number_of_speeds = 4  # MSHP always variable speed
    elif HP_info['compressor_type'] in speed_options:
        number_of_speeds = speed_options[HP_info['compressor_type']]
    elif convert(1 / eir, 'W', 'Btu/hour') <= 15:
        number_of_speeds = 1  # Single-speed for SEER <= 15
    elif convert(1 / eir, 'W', 'Btu/hour') <= 21:
        number_of_speeds = 2  # Two-speed for 15 < SEER <= 21
    else:
        number_of_speeds = 4  # Variable speed for SEER > 21

    is_heater = hvac_type == 'Heating'
    if is_heater:
        shr = None
    else:
        shr = HP_info['sensible_heat_ratio']

    if "fan_power_Watts_per_CFM" in HP_info:
        # Note: air flow rate is only used for non-dymanic HVAC models with fans, e.g., furnaces
        # airflow_cfm = hvac_ext.get(f'{hvac_type}AirflowCFM', 0)
        cfm_per_ton = 350 if is_heater else 312
        power_per_cfm = HP_info["fan_power_Watts_per_CFM"]
        aux_power = power_per_cfm * cfm_per_ton * convert(capacity, 'W', 'refrigeration_ton')

    HP = {
        'Equipment Name': name,
        'Fuel': fuel.capitalize(),
        'Capacity (W)': capacity,
        'EIR (-)': eir,
        'Rated Efficiency': efficiency_string,
        'SHR (-)': shr,
        'Conditioned Space Fraction (-)': space_fraction,
        'Number of Speeds (-)': number_of_speeds,
        'Rated Auxiliary Power (W)': aux_power,
    }

    return HP


@click.command()
@click.argument('building_id', type=int)
@click.argument('upgrade_id', type=int)
@click.argument('hp_id', type=int)
def swap_HP(
        building_id:int,
        upgrade_id:int,
        hp_id:int
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
        # Baseline simulation
        dwelling = Dwelling(**house_args)

        # Load properties from HPXML file
        properties, weather_station = load_hpxml(**house_args)
        # Load occupancy schedule and weather files
        schedule, location = load_schedule(
            properties, weather_station=weather_station, **house_args
        )
        properties["location"] = location
        # Update args for initializing Envelope and Equipment
        sim_args = {
            **house_args,
            "start_time": dwelling.start_time,  # updates time zone if necessary
            "schedule": schedule,
            "initial_schedule": schedule.loc[dwelling.start_time].to_dict(),
            "output_path": dwelling.output_path,
        }
        # initial_schedule.update(self.envelope.get_main_states())
        sim_args["envelope_model"] = dwelling.envelope
        HP1 = create_HP_equipment(get_HP_info(1), 'Cooling')
        properties['equipment']['HVAC Cooling'].update(HP1)

        # Add detailed equipment properties, including ZIP parameters
        equipment_dict = update_equipment_properties(properties, **sim_args)
        #print(equipment_dict)
        # continue at Dwelling line 139

        dwelling.simulate()
    except Exception as e:
        print(f"Simulation failed for bldg{building_id:07}-up{upgrade_id:02}: {e}")
    
if __name__ == '__main__':
    swap_HP()