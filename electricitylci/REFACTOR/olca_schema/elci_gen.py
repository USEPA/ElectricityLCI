#REFACTOR CODE##############3
import warnings
from elci import globals

import importlib
importlib.reload(globals)
#warnings.filterwarnings("ignore")
#globals.read_all_databases()


names,fuelname = globals.read_eLCI_base_data()

