# Electricity Life Cycle Inventory

THIS PROJECT IS IN ALPHA STATE AND OUTPUTS SHOULD NOT BE USED FOR LCA OR OTHER PURPOSES AT THIS TIME

Use standardized facility release data along with existing models to create regionalized life cycle inventory data for the generation,
 mix of generation, mix of consumption, and distribution of electricity to end-users for the US. 

This project is a collaboration between US EPA Office of Research and Development (USEPA) and the National Renewable Energy Laboratory (NREL),
 with support from Franklin Associates (ERG), and other federal entities, including the National Energy Technology Laboratory and
 the National Agricultural Library. The goal of the project is to create an LCI model of electricity for the [Federal LCA Commons](http://www.lcacommons.gov/catalog).
  US EPA is the lead on Phase I.

Phase I Active Team members (* = lead):
USEPA - [Wesley Ingwersen](https://github.com/WesIngwersen)*
NREL - Alberta Carpenter
ERG - [Tapajyoti Ghosh](https://github.com/TJTapajyoti), Troy Hottle, Sarah Cashman
Former contributors: Kirti Richa and Troy Hawkins (current affiliation: Argonne National Laboratory)

##Requirements for use
This project requires Python 3.x and a number of available packages. See the setup.py file.
To run the modules requires downloading/pulling these two python projects:
[Standardized Emission and Waste Inventories (StEWI)](https://github.com/usepa/standardizedinventories)
[Federal-LCA-Commons-Elementary-Flow-List](https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List)
This project folders should be put inside the directory housing this project file so they reside at the same level as the 
'electricitylci' directory.

## Disclaimer
The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis 
and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer 
has responsibility to protect the integrity , confidentiality, or availability of the information. 
Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, 
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  
The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity 
by EPA or the United States Government.
