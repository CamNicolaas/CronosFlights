import random
from typing import Optional
from rapidfuzz import process
from typing import List, Dict


def airports_iata_list() -> List[Dict[str, str]]:
    return [
        {'code': 'BHI', 'name': 'Bahía Blanca'}, {'code': 'BRC', 'name': 'Bariloche'},
        {'code': 'BUE', 'name': 'Buenos Aires'}, {'code': 'AEP', 'name': 'Buenos Aires Aeroparque '},
        {'code': 'EZE', 'name': 'Buenos Aires Ezeiza'}, {'code': 'CTC', 'name': 'Catamarca'},
        {'code': 'CRD', 'name': 'Comodoro Rivadavia'}, {'code': 'CNQ', 'name': 'Corrientes'},
        {'code': 'COR', 'name': 'Córdoba'}, {'code': 'FTE', 'name': 'El Calafate'},
        {'code': 'EQS', 'name': 'Esquel'}, {'code': 'FMA', 'name': 'Formosa'},
        {'code': 'IGR', 'name': 'Iguazu'}, {'code': 'JUJ', 'name': 'Jujuy'},
        {'code': 'IRJ', 'name': 'La Rioja'}, {'code': 'LGS', 'name': 'Malargue'},
        {'code': 'MDQ', 'name': 'Mar Del Plata'}, {'code': 'MDZ', 'name': 'Mendoza'},
        {'code': 'RLO', 'name': 'Merlo'}, {'code': 'NQN', 'name': 'Neuquén'},
        {'code': 'PRA', 'name': 'Parana'}, {'code': 'PSS', 'name': 'Posadas'},
        {'code': 'PMY', 'name': 'Puerto Madryn'}, {'code': 'RCQ', 'name': 'Reconquista'},
        {'code': 'RES', 'name': 'Resistencia'}, {'code': 'RCU', 'name': 'Rio Cuarto'},
        {'code': 'RGA', 'name': 'Rio Grande'}, {'code': 'RHD', 'name': 'Rio Hondo'},
        {'code': 'ROS', 'name': 'Rosario'}, {'code': 'RGL', 'name': 'Río Gallegos'},
        {'code': 'SLA', 'name': 'Salta'}, {'code': 'UAQ', 'name': 'San Juan'},
        {'code': 'LUQ', 'name': 'San Luis'}, {'code': 'CPC', 'name': 'San Martin de los Andes'},
        {'code': 'AFA', 'name': 'San Rafael'}, {'code': 'SFN', 'name': 'Santa Fe'},
        {'code': 'RSA', 'name': 'Santa Rosa'}, {'code': 'SDE', 'name': 'Santiago Del Estero'},
        {'code': 'REL', 'name': 'Trelew'}, {'code': 'TUC', 'name': 'Tucuman'},
        {'code': 'USH', 'name': 'Ushuaia'}, {'code': 'VDM', 'name': 'Viedma'},
        {'code': 'VLG', 'name': 'Villa Gesell'}, {'code': 'VME', 'name': 'Villa Mercedes'}
    ]


# ---------- Airports Tools ----------
def checker_airports(
    state:Optional[str] = None, 
    return_random:Optional[bool] = False
) -> dict[str, str]:

    airports_list = airports_iata_list()
    if return_random:
        return random.choice(airports_list)

    if state:
        names = [d['name'] for d in airports_list]
        match, score, index = process.extractOne(state, names)
        if score > 75:
            return airports_list[index]

    return next(x for x in airports_list if x['code'] == 'BUE')


def random_routes(
        diferent_to:Optional[str] = None
    ) -> str:

    airports = airports_iata_list()
    if diferent_to:
        airports = [a for a in airports if a['code'] != diferent_to]

    return random.choice(airports)["code"]