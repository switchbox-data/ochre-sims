"""
Code for downloading resstock building xml

Author: Alex Lee (alexlee5124@gmail.com)
Date: 3/29/2025

"""

import os
import csv
import requests
import zipfile
from datetime import date
#AWS
import boto3
from botocore.config import Config
from botocore import UNSIGNED


URL_BASE = "https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock"


def fetch_building_xml(
        state:str,
        upgrade:int,
        year=2022,
        version="resstock_amy2018_release_1.1",
):
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
        


"""# Testing functionality
if __name__ == '__main__':
    # Example use
    # Note there are more than 30,000 buildings in the upgrade list. Modify the code to only run a handful of times if looking to test functionality.
    fetch_building_xml(state="NY", upgrade=3,year=2022,version="resstock_amy2018_release_1.1")"""

