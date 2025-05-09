import os
import xmltodict
import click
import pandas as pd
from datetime import datetime, timedelta
from datetime import date
import time
import shutil
from ochre import Dwelling, Analysis, CreateFigures
from ochre.utils import default_input_path  # for using sample files

"""
Code for running simulations on OCHRE.

Author: Alex Lee (alexlee5124@gmail.com)
Date: 4/19/2025

"""


@click.command()
@click.argument('building_id', type=int)
@click.argument('upgrade_id', type=int)
@click.argument('state', type=str)

def simulate(
        building_id:int,
        upgrade_id:int,
        state:str,
        year=2022,
        version="resstock_amy2018_release_1.1",
        start_time=datetime(2007, 1, 1, 0, 0),
        time_res=timedelta(minutes=60),
        duration=timedelta(days=3),
        ):
    """
    simulate: This function runs an OCHRE simulation for a building. It filters the building by the
    building id, upgrade id, state, year, and version of the building XML file. You need to have already
    downloaded the building XML file prior to running this function.

    Input simulation parameters, such as duration, start date, simulation timestep size, etc.

    Author: Alex Lee (alexlee5124@gmail.com)
    Date: 3/29/2025
    """

    building_folder = f"bldg{building_id:07}-up{upgrade_id:02}"
    input_file_path = os.path.join(os.path.dirname(os.path.dirname(os.getcwd())), f"data/building_energy_models/{state}_{year}_{version}/{building_folder}")

    # Load XML and schedule file
    xml_file = None
    schedule_file = None
    for file in os.listdir(input_file_path):
        if file.endswith(".xml"):
            xml_file = os.path.join(input_file_path, file)
        elif file.endswith(".csv"):
            schedule_file = os.path.join(input_file_path, file)

    # Check that XML and schedule files exist. Import relevant if they do.
    if xml_file:
        # Extract weather station name if XML file exists
        weatherStation = extract_weather_station(xml_file)
    else:
        print(f"Building XML file does not exist for: bldg{building_id:07}-up{upgrade_id:02}")
        return
    if not schedule_file:
        print(f"Missing schedule file in: bldg{building_id:07}-up{upgrade_id:02}")

    # Save simulation output to /ochre-sims/data/ochre_simulation/state_year_version
    output_filepath = os.path.join(os.path.dirname(os.path.dirname(os.getcwd())), f"data/ochre_simulation/{state}_{year}_{version}/bldg{building_id:07}-up{upgrade_id:02}")
    os.makedirs(output_filepath, exist_ok=True)
    try:        
        if schedule_file:
            house = Dwelling(
                start_time=start_time,
                time_res=time_res,
                duration=duration,
                hpxml_file=xml_file,
                hpxml_schedule_file=schedule_file,
                output_path=output_filepath,
                weather_file=os.path.join(os.path.dirname(os.path.dirname(os.getcwd())),"data","weather","BuildStock_TMY3_FIPS", f"{weatherStation}.epw"),
            )
        else:
            house = Dwelling(
                start_time=start_time,
                time_res=time_res,
                duration=duration,
                hpxml_file=xml_file,
                output_path=output_filepath,
                weather_file=os.path.join(os.path.dirname(os.path.dirname(os.getcwd())),"data","weather","BuildStock_TMY3_FIPS", f"{weatherStation}.epw"),
            )
        df = house.simulate()
                
    except Exception as e:
        print(f"Simulation failed for bldg{building_id:07}-up{upgrade_id:02}: {e}")
        # Delete failed simulation file
        path = os.path.join(input_file_path, "simulation_results")
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
            except Exception as e:
                print(f"Error deleting {path}: {e}")
        else:
            print(f"The directory does not exist: {path}")
        


def extract_weather_station(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            buildingXML = xmltodict.parse(file.read())
            weather_station_name = buildingXML['HPXML']['Building']['BuildingDetails']['ClimateandRiskZones']['WeatherStation']['Name']
    except Exception as e:
        print(f"Error parsing XML {file_path}: {e}")

    return weather_station_name

if __name__ == '__main__':
    simulate()