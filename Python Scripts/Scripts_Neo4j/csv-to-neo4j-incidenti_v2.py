import csv
import os
import time
from neo4j import GraphDatabase

print("Current working directory:", os.getcwd())

# Configura la connessione a Neo4j
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "adminadmin"))

# Percorso del file CSV degli incidenti
incidents_csv = './csv_incidentiFebbraio.csv'  # Modifica con il percorso del tuo CSV

def create_node_query(label, **kwargs):
    # Filtra i parametri per rimuovere quelli vuoti
    filtered_kwargs = {key: value for key, value in kwargs.items() if value}
    param_str = ', '.join([f'{key}: ${key}' for key in filtered_kwargs.keys()])
    return f"MERGE (n:{label} {{ {param_str} }})", filtered_kwargs

def create_relationship_query(from_label, to_label, relationship_type, from_key, to_key):
    return (
        f"MATCH (a:{from_label} {{{from_key}: $from_value}}), (b:{to_label} {{{to_key}: $to_value}}) "
        f"MERGE (a)-[r:{relationship_type}]->(b)"
    )

# Leggi il file CSV degli incidenti
def read_incidents_csv(file_path):
    incidents = []
    try:
        with open(file_path, mode='r', encoding='latin-1') as incidents_file:
            reader = csv.DictReader(incidents_file, delimiter=';')  # Specifica il delimitatore
            for row in reader:
                # Pulisci le chiavi e valori del dizionario
                cleaned_row = {key.strip().replace(' ', '_').lower(): value.strip() if value is not None else '' for key, value in row.items()}
                incidents.append(cleaned_row)
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
        exit(1)
    return incidents

print('Data extraction complete\n')

# Inserisci i nodi e relazioni in Neo4j
def insert_data_to_neo4j(incidents):
    total_incidents = len(incidents)
    last_print_time = time.time()  # Tempo dell'ultimo aggiornamento della percentuale
    with driver.session() as session:
        for i, incident in enumerate(incidents, start=1):
            current_time = time.time()
            elapsed_time = current_time - last_print_time
            
            # Stampa la percentuale ogni secondo
            if elapsed_time >= 1:
                percent_complete = (i / total_incidents) * 100
                print(f"Processing incident {i}/{total_incidents} ({percent_complete:.2f}% complete)")
                last_print_time = current_time  # Aggiorna il tempo dell'ultimo aggiornamento

            # Creazione nodo Incidente
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
            session.run(incident_query, incident_params)
           #print(f"Created Incidente node: {incident.get('protocollo')}")

            # Creazione nodo Veicolo
            vehicle_query, vehicle_params = create_node_query(
                'Veicolo',
                protocollo=incident.get('protocollo'),
                progressivo=incident.get('progressivo'),
                tipoveicolo=incident.get('tipoveicolo'),
                statoveicolo=incident.get('statoveicolo'),
                statoairbag=incident.get('airbag')
            )
            session.run(vehicle_query, vehicle_params)
            #print(f"Created Veicolo node: {incident.get('protocollo')}")
            # Creazione nodo Strada
            road_query, road_params = create_node_query(
                'Strada',
                protocollo=incident.get('protocollo'),
                strada1=incident.get('strada1'),
                localizzazione=incident.get('localizzazione1') + ' ' + incident.get('localizzazione2'),
                particolarita=incident.get('particolaritastrade'),
                tipostrada=incident.get('tipostrada'),
                fondostradale=incident.get('fondostradale'),
                pavimentazione=incident.get('pavimentazione'),
                segnaletica=incident.get('segnaletica')
            )
            session.run(road_query, road_params)
            #print(f"Created Strada node: {incident.get('protocollo')}")

            # Creazione nodo Persona con attributo aggiuntivo tipopersona
            person_query, person_params = create_node_query(
                'Persona',
                protocollo=incident.get('protocollo'),
                sesso=incident.get('sesso'),
                tipolesione=incident.get('tipolesione'),
                casco_cintura=incident.get('cinturacascoutilizzato'),
                deceduto=incident.get('deceduto'),
                deceduto_dopo=incident.get('deceduto_dopo'),
                tipopersona=incident.get('tipopersona')  # Nuovo attributo
            )
            session.run(person_query, person_params)
            #print(f"Created Persona node: {incident.get('protocollo')}")
            # Creazione delle relazioni tra nodi
            session.run(
                create_relationship_query('Incidente', 'Veicolo', 'Coinvolge_veicolo', 'protocollo', 'protocollo'),
                {'from_value': incident.get('protocollo'), 'to_value': incident.get('protocollo')}
            )
            #print(f"Created relationship Coinvolge_veicolo for protocollo: {incident.get('protocollo')}")

            session.run(
                create_relationship_query('Incidente', 'Strada', 'Occorso_su', 'protocollo', 'protocollo'),
                {'from_value': incident.get('protocollo'), 'to_value': incident.get('protocollo')}
            )
            #print(f"Created relationship Occorso_su for protocollo: {incident.get('protocollo')}")

            session.run(
                create_relationship_query('Incidente', 'Persona', 'Coinvolge_persona', 'protocollo', 'protocollo'),
                {'from_value': incident.get('protocollo'), 'to_value': incident.get('protocollo')}
            )
            #print(f"Created relationship Coinvolge_persona for protocollo: {incident.get('protocollo')}")

            session.run(
                create_relationship_query('Veicolo', 'Strada', 'Su', 'protocollo', 'protocollo'),
                {'from_value': incident.get('protocollo'), 'to_value': incident.get('protocollo')}
            )
            #print(f"Created relationship Su for protocollo: {incident.get('protocollo')}")

# Inserisci i nodi e le relazioni
insert_data_to_neo4j(read_incidents_csv(incidents_csv))

# Chiudi la connessione al database
driver.close()
print('All data has been processed and the connection to Neo4j is closed.')