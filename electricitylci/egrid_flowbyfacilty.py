import stewi

def get_egrid_flowbyfacility(egrid_year):
    # Get inventory data to get net generation per facility
    egrid_flowbyfacility = stewi.getInventory("eGRID", egrid_year)
    
    # Peek at it
    egrid_flowbyfacility.head(50)
    return egrid_flowbyfacility