# ochre-sims

## Installation
This repository relies on python, duckdb, and just. The easiest way to install these is to use a devcontainer in vscode. To do so,

* Install Docker Desktop 
* Install Visual Studio Code
* Install the Devcontainer vscode extension
* Clone this repository
* Open repository in vscode
* Click on "Reopen in Container" pop up
  * Or, type <kbd>⌘</kbd> + <kbd>⇧</kbd> + <kbd>p</kbd> → `Dev Containers: Reopen in Container`

---------------------------------

Updates made on 3/31:

Ochre-sims notes:

Installation/requirements issue:
- Had to install homebrew, so I could install Justfiles version 1.40.0
- Had to install duckdb using homebrew

load_2022_release.sql:
- Changed all path description where it says “reports” to “ochre-sims”
- Modified “load_2022_release.sql” file wherever it says “resstock” to “building_energy_models”

Justfile:
- Line 3 roots: changed from “$HOME/ochre-sims” to “/workspaces/ochre-sims”
- Changed line 4 and 5 (lib and data paths) to “lib/building_energy_models” and “data/building_energy_models”
- Added “mkdir -p {{data}}/{{dir_2022}}” to line 57 in annual_2022. Without it, the duckdb code on line 61 causes issues due to nonexistent directory

—————————————————————————————————

Explanation for Python code for downloading building xml
- Run “resstock_fetch_building_xml.py” inside ochre-sims/lib/building_energy_models
- fetch_building_xml() function takes as input: state, upgrade number, year (default = 2022), version (default = "resstock_amy2018_release_1.1”)
- This function downloads the metadata .csv file, which contains the building_id and upgrade_id, filtered by the input state, upgrade, year, and version
- Based on this metadata, it downloads all the corresponding building .xml files and schedule files if they exist. Some building id’s don’t have schedule files. These are saved out to ochre-sims/data/building_energy_models/2022_resstock_amy2018_release_1.1
- From this .xml file, the weather station id is extracted
- All the weather data in .epw format is stored in ochre-sims/data/weather/BuildStock_TMY3_FIPS/

—————————————————————————————————

Ochre/utils/schedule.py
- Added "hot_tub_pump": "Hot Tub Pump”, “hot_tub_heater": "Hot Tub Heater” to “Power” SCHEDULE_NAMES. Without it, it can’t read these two power sources from the schedule file.