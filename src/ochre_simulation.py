import os
import sys
import xmltodict
import click
from datetime import datetime, timedelta
import shutil
from ochre import Dwelling

"""
Code for running simulations on OCHRE.

Author: Alex Lee (alexlee5124@gmail.com)
Date: 4/19/2025

"""

def extract_weather_station(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            buildingXML = xmltodict.parse(file.read())
            weather_station_name = buildingXML['HPXML']['Building']['BuildingDetails']['ClimateandRiskZones']['WeatherStation']['Name']
    except Exception as e:
        print(f"Error parsing XML {file_path}: {e}")

    return weather_station_name

@click.command()
@click.argument('building_id', type=int)
@click.argument('upgrade_id', type=int)
@click.argument('state', type=str)
@click.argument('start_time', default=datetime(2007, 1, 1, 0, 0),type=click.DateTime(formats=["%Y/%m/%d"]))
@click.argument('time_res', default=60,type=int)
@click.argument('duration', default=3,type=int)
@click.option('--hpxml_year', default=2024,type=int, help="Release year of HPXML files")
@click.option('--version', default="resstock_amy2018_release_1.1",type=str, help="Relese version of the HPXML metadata")
def simulate_dwelling(
        building_id:int,
        upgrade_id:int,
        state:str,
        hpxml_year=2024,
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
    data_folder_path = os.path.join(os.path.dirname(os.path.dirname(os.getcwd())), "data")

    input_filepath = os.path.join(data_folder_path, "building_energy_models/"
                                  f"{state}_{hpxml_year}_{version}/{building_folder}")
    # Load XML and schedule file
    xml_file = os.path.join(input_filepath, f"bldg{building_id:07}-up{upgrade_id:02}.xml")
    try:
        weather_station = extract_weather_station(xml_file)
        weather_file=os.path.join(data_folder_path,f"weather/BuildStock_TMY3_FIPS/{weather_station}.epw")
    except Exception as e:
        print(f"Building XML file does not exist for {xml_file} : {e}")
        sys.exit(1)

    schedule_file = os.path.join(input_filepath, f"bldg{building_id:07}-up{upgrade_id:02}_schedule.csv")
    if not os.path.exists(schedule_file):
        print(f"Schedule file does not exist: {schedule_file}")
        sys.exit(1)

    # Save simulation output to /ochre-sims/data/ochre_simulation/state_year_version
    output_filepath = os.path.join(data_folder_path, 
                        f"output/ochre_simulation/{state}_{hpxml_year}_{version}",
                        f"bldg{building_id:07}-up{upgrade_id:02}")
    os.makedirs(output_filepath, exist_ok=True)
    try:
        house = Dwelling(
                start_time=start_time,
                time_res=time_res,
                duration=duration,
                hpxml_file=xml_file,
                hpxml_schedule_file=schedule_file,
                output_path=output_filepath,
                weather_file=weather_file,
            )
        house.simulate()
                
    except Exception as e:
        print(f"Simulation failed for bldg{building_id:07}-up{upgrade_id:02}: {e}")
        # Delete failed simulation file
        path = os.path.join(input_filepath, "simulation_results")
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
            except Exception as e:
                print(f"Error deleting {path}: {e}")
        else:
            print(f"The directory does not exist: {path}")
    

if __name__ == '__main__':
    simulate_dwelling()