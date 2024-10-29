import os
import re
import csv
from neo4j import GraphDatabase
from tqdm import tqdm

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "adminadmin"))
db_name = "version1"

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
                    csv_files.append((year, filename, os.path.join(dirpath, filename)))

    csv_files.sort(key=lambda x: (int(x[0]), month_order_key(x[1])))
    return [file_path for _, _, file_path in csv_files]

def create_indexes(session):
    print("Creating indexes...")
    session.run("CREATE INDEX IF NOT EXISTS FOR (i:Incidente) ON (i.protocollo)")
    session.run("CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.protocollo)")
    session.run("CREATE INDEX IF NOT EXISTS FOR (v:Veicolo) ON (v.protocollo, v.progressivo)")
    session.run("CREATE INDEX IF NOT EXISTS FOR (p:Persona) ON (p.idpersona)")
    print("Indexes created.")

def create_database_if_not_exists(session, db_name):
    result = session.run("SHOW DATABASES")
    databases = [record["name"] for record in result]
    if db_name not in databases:
        session.run(f"CREATE DATABASE {db_name}")
        print(f"Database '{db_name}' created.")
    else:
        print(f"Database '{db_name}' already exists.")

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

def read_incidents_csv(file_path):
    incidents = []
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

def insert_data_to_neo4j(file_path, idpersona_counter, batch_size=100):
    print(f"Processing dataset: {file_path}")
    incidents = read_incidents_csv(file_path)
    batch = []

    with driver.session() as session:
        create_database_if_not_exists(session, db_name)

        with driver.session(database=db_name) as session:
            create_indexes(session)
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
                batch.append((incident_query, incident_params))

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
                batch.append((road_query, road_params))

                occorso_su_query = create_relationship_query('Incidente', 'Strada', 'OCCORSO_SU', ['protocollo'], ['protocollo'])
                batch.append((occorso_su_query, {'from_protocollo': incident.get('protocollo'), 'to_protocollo': incident.get('protocollo')}))

                tipopersona = incident.get('tipopersona')
                if not (tipopersona and tipopersona.lower() == 'pedone'):
                    vehicle_query, vehicle_params = create_node_query(
                        'Veicolo',
                        protocollo=incident.get('protocollo'),
                        progressivo=incident.get('progressivo'),
                        statoveicolo=incident.get('statoveicolo'),
                        statoairbag=incident.get('airbag'),
                        tipoveicolo=incident.get('tipoveicolo')
                    )
                    batch.append((vehicle_query, vehicle_params))

                    person_query, person_params = create_node_query(
                        'Persona',
                        idpersona=idpersona_counter,
                        protocollo=incident.get('protocollo'),  
                        sesso=incident.get('sesso'),
                        tipolesione=incident.get('tipolesione'),
                        casco_cintura=incident.get('cinturacascoutilizzato'),
                        deceduto=incident.get('deceduto'),
                        deceduto_dopo=incident.get('deceduto_dopo'),
                        tipopersona=incident.get('tipopersona')
                    )
                    batch.append((person_query, person_params))

                    batch.append((create_relationship_query('Incidente', 'Veicolo', 'COINVOLGE_VEICOLO', ['protocollo'], ['protocollo', 'progressivo']),
                                  {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_protocollo': incident.get('protocollo'), 'to_progressivo': incident.get('progressivo')}))

                    batch.append((create_relationship_query('Incidente', 'Persona', 'COINVOLGE_PERSONA', ['protocollo'], ['idpersona']),
                                  {'from_protocollo': incident.get('protocollo'), 'to_idpersona': idpersona_counter}))

                    batch.append((create_relationship_query('Veicolo', 'Strada', 'SU', ['protocollo', 'progressivo'], ['protocollo', 'nome']),
                                  {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_protocollo': incident.get('protocollo'), 'to_nome': incident.get('strada1')}))

                    batch.append((create_relationship_query('Veicolo', 'Persona', 'GUIDATO_DA', ['protocollo', 'progressivo'], ['protocollo', 'idpersona']),
                                  {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_protocollo': incident.get('protocollo'), 'to_idpersona': idpersona_counter}))

                    idpersona_counter += 1

                if len(batch) >= batch_size:
                    for query, params in batch:
                        session.run(query, params)
                    batch = []

            if batch:
                for query, params in batch:
                    session.run(query, params)

    return idpersona_counter  # Return the updated counter

if __name__ == "__main__":
    incidents_csv_directory = './Datasets/'
    incidents_csv_files = get_csv_files(incidents_csv_directory)

    idpersona_counter = 1  

    for csv_file in incidents_csv_files:
        idpersona_counter = insert_data_to_neo4j(csv_file, idpersona_counter)

    driver.close()
    print('All data has been processed and the connection to Neo4j is closed.')
