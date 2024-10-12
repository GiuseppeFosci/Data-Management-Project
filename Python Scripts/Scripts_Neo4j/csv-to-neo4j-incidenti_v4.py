import csv 
import os
import time
from neo4j import GraphDatabase

print("Current working directory:", os.getcwd()) # Stampiamo la directory di lavoro corrente

# Configuro la connessione a Neo4j
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "adminadmin"))

# Lista dei percorsi dei file CSV degli incidenti
incidents_csv_files = [
    #inidenti 2020
    './Datasets/2020/csv_incidentiGennaio.csv',
    
    
    #Incidenti 2021
    
]

def create_node_query(label, **kwargs):  # Creo una query Cypher per inserire (o unire) un nodo con un'etichetta specifica.
    filtered_kwargs = {key: value for key, value in kwargs.items() if value}
    param_str = ', '.join([f'{key}: ${key}' for key in filtered_kwargs.keys()])
    return f"MERGE (n:{label} {{ {param_str} }})", filtered_kwargs

def create_relationship_query(from_label, to_label, relationship_type, from_key, to_key): # Creo una query Cypher per stabilire una relazione tra due nodi.
    return (
        f"MATCH (a:{from_label} {{{from_key}: $from_value}}), (b:{to_label} {{{to_key}: $to_value}}) " # Cerco i nodi esistenti basandomi sulle etichette e le proprietà specificate.
        f"MERGE (a)-[r:{relationship_type}]->(b)"    # Creo una relazione diretta tra il nodo a ed il nodo b, se la relazione esiste già, non la ricrea. Se non esiste, la crea.
    )

def read_incidents_csv(file_paths): # Leggo più file CSV e restituisco una lista di dizionari, ciascuno rappresentante un incidente.
    incidents = []  # Inizializzo una lista vuota chiamata Incidents, dove memorizzo i dizionari con i dati degli incidenti
    for file_path in file_paths:
        try:
            with open(file_path, mode='r', encoding='latin-1') as incidents_file:  # Apro il file csv passato da file_path in modalità lettura (mode='r') e lo decodifica usando la codifica latin-1
                reader = csv.DictReader(incidents_file, delimiter=';')  # Uso csv.DictReader per leggere il file CSV come un dizionario per ogni riga, dove le chiavi sono i nomi delle colonne
                for row in reader:
                    # Pulisci le chiavi e valori del dizionario
                    cleaned_row = {key.strip().replace(' ', '_').lower(): value.strip() if value is not None else '' for key, value in row.items()} # Creo una versione "pulita" della riga 
                    incidents.append(cleaned_row)
        except Exception as e:
            print(f"An error occurred while reading the file {file_path}: {e}")
            exit(1)
    return incidents

def clear_graph(session):
    # Cancella tutti i nodi e relazioni nel grafo
    session.run("MATCH (n) DETACH DELETE n")
    print("Graph cleared")

def insert_data_to_neo4j(incidents):
    total_incidents = len(incidents)
    last_print_time = time.time()  # Tempo dell'ultimo aggiornamento della percentuale
    idpersona_counter = 1  # Inizializzo il contatore per idpersona

    with driver.session() as session:
        clear_graph(session)  # Aggiungi questa riga per pulire il database
        for i, incident in enumerate(incidents, start=1):
            current_time = time.time()
            elapsed_time = current_time - last_print_time
            
            # Mi serve per stampare i progressi dei nodi caricati ogni secondo
            if elapsed_time >= 1:
                percent_complete = (i / total_incidents) * 100
                print(f"Processing incident {i}/{total_incidents} ({percent_complete:.2f}% complete)")
                last_print_time = current_time  # Aggiorna il tempo dell'ultimo aggiornamento

            # Creo il nodo Incidente
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

            # Creo nodo Veicolo
            vehicle_query, vehicle_params = create_node_query(
                'Veicolo',
                protocollo=incident.get('protocollo'),
                progressivo=incident.get('progressivo'),
                tipoveicolo=incident.get('tipoveicolo'),
                statoveicolo=incident.get('statoveicolo'),
                statoairbag=incident.get('airbag')
            )
            session.run(vehicle_query, vehicle_params)

            # Creo nodo Strada
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

            # Creazione nodo Persona con attributo aggiuntivo tipopersona e idpersona autoincrementale
            person_query, person_params = create_node_query(
                'Persona',
                protocollo=incident.get('protocollo'),
                idpersona=idpersona_counter,  # Assegna il valore del contatore
                sesso=incident.get('sesso'),
                tipolesione=incident.get('tipolesione'),
                casco_cintura=incident.get('cinturacascoutilizzato'),
                deceduto=incident.get('deceduto'),
                deceduto_dopo=incident.get('deceduto_dopo'),
                tipopersona=incident.get('tipopersona')  # Nuovo attributo
            )
            session.run(person_query, person_params)

            # Incrementa il contatore per il prossimo idpersona
            idpersona_counter += 1

            # Creazione delle relazioni tra nodi
            session.run(
                create_relationship_query('Incidente', 'Veicolo', 'Coinvolge_veicolo', 'protocollo', 'protocollo'),
                {'from_value': incident.get('protocollo'), 'to_value': incident.get('protocollo')}
            )

            session.run(
                create_relationship_query('Incidente', 'Strada', 'Occorso_su', 'protocollo', 'protocollo'),
                {'from_value': incident.get('protocollo'), 'to_value': incident.get('protocollo')}
            )

            session.run(
                create_relationship_query('Incidente', 'Persona', 'Coinvolge_persona', 'protocollo', 'protocollo'),
                {'from_value': incident.get('protocollo'), 'to_value': incident.get('protocollo')}
            )

            session.run(
                create_relationship_query('Veicolo', 'Strada', 'Su', 'protocollo', 'protocollo'),
                {'from_value': incident.get('protocollo'), 'to_value': incident.get('protocollo')}
            )

# Leggi e combina i dati da tutti i file CSV
all_incidents = read_incidents_csv(incidents_csv_files)

# Inserisci i nodi e le relazioni
insert_data_to_neo4j(all_incidents)

# Chiudi la connessione al database
driver.close()
print('All data has been processed and the connection to Neo4j is closed.')
