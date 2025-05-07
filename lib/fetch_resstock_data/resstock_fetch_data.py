import os
import csv
import requests
import zipfile
from datetime import date
from ochre import Analysis
#AWS
import boto3
from botocore.config import Config
from botocore import UNSIGNED

"""
Code for downloading resstock data such as building xml, previously performed simulation timeseries

Author: Alex Lee (alexlee5124@gmail.com)
Date: 3/29/2025

"""


URL_BASE = "https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock"

def fetch_building_xml(
        state:str,
        upgrade:int,
        year=2022,
        version="resstock_amy2018_release_1.1",
):
    """
    fetch_building_xml: This function downloads the building XML file (which holds meta data for a particular building, such as
    building envelope info, weather station, type of HVAC equipment, etc.). the XML file is filtered by state, upgrade id, 
    year of published date, and version. This function will download all the available building xml files for that state, upgrade id, published 
    year, and version.

    Author: Alex Lee (alexlee5124@gmail.com)
    Date: 3/29/2025
    """

    # Create directories for saving data. Save building xml's to /ochre-sims/data/building_energy_models/state_year_version
    building_data_path = os.path.join(os.path.dirname(os.path.dirname(os.getcwd())), f"data/building_energy_models/{state}_{year}_{version}")
    os.makedirs(building_data_path, exist_ok=True)

    # Fetch and save metadata information for given state, upgrade, and year. Metadata contains building id and upgrade id
    building_url = f"{URL_BASE}/{year}/{version}/metadata_and_annual_results/by_state/state={state}/csv/{state}_upgrade{upgrade:02d}_metadata_and_annual_results.csv"
    response = requests.get(building_url)
    response.raise_for_status()
    with open(f"{building_data_path}/{state}_upgrade{upgrade:02d}_metadata_and_annual_results.csv", "wb") as file:
        file.write(response.content)

    # Read in building metadata, extract out building and upgrade id. Save these to a list of dictionaries with keys "bldg_id" and "upgrade"
    buildingList = []
    with open(f"{building_data_path}/{state}_upgrade{upgrade:02d}_metadata_and_annual_results.csv", mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        headers = reader.fieldnames[:2]
        for row in reader:
            buildingList.append({headers[0]: row[headers[0]], headers[1]: row[headers[1]]})
    os.remove(f"{building_data_path}/{state}_upgrade{upgrade:02d}_metadata_and_annual_results.csv") # Remove metadata .csv

    # Download buiilding XML files
    date_string = date.today().strftime("%Y%m%d")
    for ResStockBuilding in buildingList:
        building_id = int(ResStockBuilding["bldg_id"])
        upgrade_id = int(ResStockBuilding["upgrade"])
        # Create new directory for each building
        current_building_path = f"{building_data_path}/bldg{building_id:07}-up{upgrade_id:02}"
        os.makedirs(current_building_path, exist_ok=True)
        # Download resstock building xml and schedule file. Note that some buildings have missing schedule files.
        oedi_path = "/".join(["nrel-pds-building-stock","end-use-load-profiles-for-us-building-stock",str(year),
                version,"building_energy_models",f"upgrade={upgrade_id}",f"bldg{building_id:07}-up{upgrade_id:02}.zip",])
        s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        zip_filepath = f"{current_building_path}/bldg{building_id:07}-up{upgrade_id:02}.zip"
        s3_client.download_file("oedi-data-lake", oedi_path, zip_filepath)
        # Extract zip file
        with zipfile.ZipFile(zip_filepath, "r") as zip_ref:
            zip_ref.extract("in.xml",current_building_path)
            if "schedules.csv" in zip_ref.namelist():
                zip_ref.extract("schedules.csv",current_building_path)
        os.remove(zip_filepath) # Remove zip file
        os.rename(f"{current_building_path}/in.xml", f"{current_building_path}/bldg{building_id:07}-up{upgrade_id:02}_{date_string}.xml")
        if os.path.exists(f"{current_building_path}/schedules.csv"):
            os.rename(f"{current_building_path}/schedules.csv", f"{current_building_path}/bldg{building_id:07}-up{upgrade_id:02}_schedule_{date_string}.csv")


def fetch_resstock_timeseries(
        building_id:int,
        upgrade_id:int,
        state=str,
        year=2022,
        version="resstock_tmy3_release_1",
):
    """
    fetch_resstock_timeseries: This function fetches previously performed ResStock simulations published by NREL. These files are filtered
    by building number, upgrade id, state,

    Author: Alex Lee (alexlee5124@gmail.com)
    Date: 4/19/2025
    """   

    # Download timeseries data
    oedi_path = "/".join(["nrel-pds-building-stock","end-use-load-profiles-for-us-building-stock",f"{year}",
            f"{version}","timeseries_individual_buildings","by_state",f"upgrade={upgrade_id}",f"state={state}",f"{building_id}-{upgrade_id}.parquet",]) # AWS data access url
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    # Save downloaded file out to /ochre-sims/data/resstock_timeseries/state_year_version
    building_data_path = os.path.join(os.path.dirname(os.path.dirname(os.getcwd())), f"data/resstock_timeseries/{state}_{year}_{version}/bldg{building_id:07}-up{upgrade_id:02}")
    os.makedirs(building_data_path, exist_ok=True)
    zip_filepath = f"{building_data_path}/{building_id}-{upgrade_id}.parquet"    # Save downloaded file out to /ochre-sims/data/resstock_timeseries/state_year_version
    s3_client.download_file("oedi-data-lake", oedi_path, zip_filepath)
    # Transcribe parquet file of the timeseries to a .csv file
    resstock_df = Analysis.load_timeseries_file(f"{building_data_path}/{building_id}-{upgrade_id}.parquet")
    resstock_df.reset_index().to_csv(f"{building_data_path}/resstock_timeseries_{building_id}-{upgrade_id}.csv", index=False)




"""# Testing functionality
if __name__ == '__main__':
    # Example use
    # Note there are more than 30,000 buildings in the upgrade list. Modify the code to only run a handful of times if looking to test functionality.
    #fetch_building_xml(state="NY", upgrade=3,year=2022,version="resstock_amy2018_release_1.1")
    fetch_resstock_timeseries(72,3,"NY",2022)"""

