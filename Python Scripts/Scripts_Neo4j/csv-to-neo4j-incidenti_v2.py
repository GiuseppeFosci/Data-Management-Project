import csv
import os
import re
from neo4j import GraphDatabase
from tqdm import tqdm  

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "adminadmin"))
db_name = "version2"
def month_order_key(filename):
    month_map = {
        "Gennaio": 1,
        "Febbraio": 2,
        "Marzo": 3,
        "Aprile": 4,
        "Maggio": 5,
        "Giugno": 6,
        "Luglio": 7,
        "Agosto": 8,
        "Settembre": 9,
        "Ottobre": 10,
        "Novembre": 11,
        "Dicembre": 12
    }
    
    month = re.search(r'csv_incidenti(.*?).csv', filename)  
    if month:
        month_name = month.group(1).strip()  
        return month_map.get(month_name, 0)  
    return 0  

def get_csv_files(base_directory):
    csv_files = []
    for dirpath, _, filenames in os.walk(base_directory):
        year = os.path.basename(dirpath) 
        if year.isdigit(): 
            for filename in filenames:
                if filename.endswith('.csv'):
                    csv_files.append((year, filename)) 

    csv_files.sort(key=lambda x: (int(x[0]), month_order_key(x[1])))  
    return [os.path.join(base_directory, x[0], x[1]) for x in csv_files]  

base_directory = './Datasets'
incidents_csv_files = get_csv_files(base_directory)

def create_database_if_not_exists(session, db_name):
    result = session.run("SHOW DATABASES")
    databases = [record["name"] for record in result]
    if db_name not in databases:
        session.run(f"CREATE DATABASE {db_name}")
        print(f"Database '{db_name}' created.")
    else:
        print(f"Database '{db_name}' already exists.")

def clear_graph(session):
    session.run("MATCH (n) DETACH DELETE n")
    print("Graph cleared")

def create_node_query(label, **kwargs):
    filtered_kwargs = {key: value for key, value in kwargs.items() if value}
    param_str = ', '.join([f'{key}: ${key}' for key in filtered_kwargs.keys()])
    return f"MERGE (n:{label} {{ {param_str} }})", filtered_kwargs

def create_relationship_query(from_label, to_label, relationship_type, from_keys, to_keys):
    from_str = ', '.join([f'{key}: $from_{key}' for key in from_keys])
    to_str = ', '.join([f'{key}: $to_{key}' for key in to_keys])
    return (
        f"MATCH (a:{from_label} {{{from_str}}}), (b:{to_label} {{{to_str}}}) "
        f"MERGE (a)-[r:{relationship_type}]->(b)"
    )

def read_incidents_csv(file_paths):
    incidents = []
    for file_path in file_paths:
        try:
            with open(file_path, mode='r', encoding='latin-1') as incidents_file:
                reader = csv.DictReader(incidents_file, delimiter=';')
                for row in reader:
                    cleaned_row = {key.strip().replace(' ', '_').lower(): value.strip() if value is not None else '' for key, value in row.items()}
                    incidents.append(cleaned_row)
        except Exception as e:
            print(f"An error occurred while reading the file {file_path}: {e}")
            exit(1)
    return incidents

def insert_data_to_neo4j(incidents):
    idpersona_counter = 1

    with driver.session() as session:  
        # Creiamo il database prima di iniziare a utilizzare sessioni specifiche per il database
        create_database_if_not_exists(session, "version2") 
        
        # Usa una sessione per il database specifico
        with driver.session(database="version2") as db_session:  
            clear_graph(db_session)

            for incident in tqdm(incidents, desc="Processing incidents", unit="incident"):
                incident_query, incident_params = create_node_query(
                    'Incidente',
                    protocollo=incident.get('protocollo'),
                    dataincidente=incident.get('dataoraincidente'),
                    chilometrica=incident.get('chilometrica'),
                    natura=incident.get('naturaincidente'),
                    gruppo=incident.get('gruppo'),
                    traffico=incident.get('traffico'),
                    condizioneatm=incident.get('condizioneatmosferica'),
                    visibilita=incident.get('visibilita'),
                    illuminazione=incident.get('illuminazione'),
                    numero_feriti=incident.get('num_feriti'),
                    numero_illesi=incident.get('num_illesi'),
                    numero_morti=incident.get('num_morti'),
                    longitudine=incident.get('longitude'),
                    latitudine=incident.get('latitude')
                )
                db_session.run(incident_query, incident_params)

                road_query, road_params = create_node_query(
                    'Strada',
                    protocollo=incident.get("protocollo"),
                    nome=incident.get('strada1'),
                    localizzazione=incident.get('localizzazione1') + ' ' + incident.get('localizzazione2'),
                    particolarita=incident.get('particolaritastrade'),
                    tipostrada=incident.get('tipostrada'),
                    fondostradale=incident.get('fondostradale'),
                    pavimentazione=incident.get('pavimentazione'),
                    segnaletica=incident.get('segnaletica')
                )
                db_session.run(road_query, road_params)

                db_session.run(
                    create_relationship_query('Incidente', 'Strada', 'OCCORSO_SU', ['protocollo'], ['protocollo']),
                    {'from_protocollo': incident.get('protocollo'), 'to_protocollo': incident.get('protocollo')}
                )

                tipopersona = incident.get('tipopersona')
                if not (tipopersona and tipopersona.lower() == 'pedone'):
                    vehicle_query, vehicle_params = create_node_query(
                        'Veicolo',
                        protocollo=incident.get('protocollo'),
                        progressivo=incident.get('progressivo'),
                        statoveicolo=incident.get('statoveicolo'),
                        statoairbag=incident.get('airbag')
                    )
                    db_session.run(vehicle_query, vehicle_params)

                    vehicle_type_query, vehicle_type_params = create_node_query(
                        'TipoVeicolo',
                        nome=incident.get('tipoveicolo')
                    )
                    db_session.run(vehicle_type_query, vehicle_type_params)

                    person_query, person_params = create_node_query(
                        'Persona',
                        idpersona=idpersona_counter,
                        sesso=incident.get('sesso'),
                        tipolesione=incident.get('tipolesione'),
                        casco_cintura=incident.get('cinturacascoutilizzato'),
                        deceduto=incident.get('deceduto'),
                        deceduto_dopo=incident.get('deceduto_dopo'),
                        tipopersona=incident.get('tipopersona')
                    )
                    db_session.run(person_query, person_params)

                    idpersona_counter += 1

                    db_session.run(
                        create_relationship_query('Incidente', 'Veicolo', 'COINVOLGE_VEICOLO', ['protocollo', 'progressivo'], ['protocollo', 'progressivo']),
                        {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_protocollo': incident.get('protocollo'), 'to_progressivo': incident.get('progressivo')}
                    )

                    db_session.run(
                        create_relationship_query('Incidente', 'Persona', 'COINVOLGE_PERSONA', ['protocollo'], ['idpersona']),
                        {'from_protocollo': incident.get('protocollo'), 'to_idpersona': idpersona_counter - 1}
                    )

                    db_session.run(
                        create_relationship_query('Veicolo', 'TipoVeicolo', 'TIPO', ['protocollo', 'progressivo'], ['nome']),
                        {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_nome': incident.get('tipoveicolo')}
                    )

                    db_session.run(
                        create_relationship_query('Veicolo', 'Strada', 'SU', ['protocollo', 'progressivo'], ['protocollo', 'nome']),
                        {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_protocollo': incident.get('protocollo'), 'to_nome': incident.get('strada1')}
                    )

                    db_session.run(
                        create_relationship_query('Veicolo', 'Persona', 'GUIDATO_DA', ['protocollo', 'progressivo'], ['protocollo', 'idpersona']),
                        {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_protocollo': incident.get('protocollo'), 'to_idpersona': idpersona_counter - 1}
                    )

insert_data_to_neo4j(read_incidents_csv(incidents_csv_files))

driver.close()
print('All data has been processed and the connection to Neo4j is closed.')
