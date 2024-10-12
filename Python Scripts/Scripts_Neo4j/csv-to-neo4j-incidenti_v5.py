import csv
import os
import time
from neo4j import GraphDatabase
#V5 Modifiche
#Creazione del nodo TipoVeicolo a cui è collegato con Veicolo tramite l'edge "TIPO", in modo tale da aumentare la "connetività" del grafo.

# Configurazione della connessione a Neo4j
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "adminadmin"))

# Percorso del file CSV degli incidenti
incidents_csv = '../Datasets/csv_prova.csv'

def clear_graph(session):
    # Cancello tutti i nodi e le relazioni nel grafo
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

def read_incidents_csv(file_path):
    incidents = []
    try:
        with open(file_path, mode='r', encoding='latin-1') as incidents_file:
            reader = csv.DictReader(incidents_file, delimiter=';')
            for row in reader:
                cleaned_row = {key.strip().replace(' ', '_').lower(): value.strip() if value is not None else '' for key, value in row.items()}
                incidents.append(cleaned_row)
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
        exit(1)
    return incidents

def insert_data_to_neo4j(incidents):
    total_incidents = len(incidents)
    start_time = time.time()
    last_print_time = start_time
    idpersona_counter = 1

    with driver.session() as session:
        clear_graph(session)

        for i, incident in enumerate(incidents, start=1):
            current_time = time.time()
            elapsed_time = current_time - last_print_time

            if elapsed_time >= 1:
                percent_complete = (i / total_incidents) * 100
                elapsed_since_start = current_time - start_time
                avg_time_per_incident = elapsed_since_start / i
                remaining_incidents = total_incidents - i
                estimated_remaining_time = avg_time_per_incident * remaining_incidents

                minutes, seconds = divmod(estimated_remaining_time, 60)
                print(f"Processing incident {i}/{total_incidents} ({percent_complete:.2f}% complete). "
                      f"Estimated time remaining: {int(minutes)}m {int(seconds)}s.")
                
                last_print_time = current_time

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

            # Creazione nodo Veicolo
            vehicle_query, vehicle_params = create_node_query(
                'Veicolo',
                protocollo=incident.get('protocollo'),
                progressivo=incident.get('progressivo'),
                statoveicolo=incident.get('statoveicolo'),
                statoairbag=incident.get('airbag')
            )
            session.run(vehicle_query, vehicle_params)

            # Creazione nodo TipoVeicolo
            vehicle_type_query, vehicle_type_params = create_node_query(
                'TipoVeicolo',
                nome=incident.get('tipoveicolo')
            )
            session.run(vehicle_type_query, vehicle_type_params)

            # Creazione nodo Strada
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
            session.run(road_query, road_params)

            # Creazione nodo Persona con attributo aggiuntivo tipopersona e idpersona autoincrementale
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
            session.run(person_query, person_params)

            # Incrementa il contatore per il prossimo idpersona
            idpersona_counter += 1

            # Creazione delle relazioni tra nodi
            session.run(
                create_relationship_query('Incidente', 'Veicolo', 'COINVOLGE_VEICOLO', ['protocollo', 'progressivo'], ['protocollo', 'progressivo']),
                {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_protocollo': incident.get('protocollo'), 'to_progressivo': incident.get('progressivo')}
            )

            session.run(
                create_relationship_query('Incidente', 'Strada', 'OCCORSO_SU', ['protocollo'], ['protocollo']),
                {'from_protocollo': incident.get('protocollo'), 'to_protocollo': incident.get('protocollo')}
            )
           
            # Relazione tra Incidente e Persona (Coinvolge_persona)
            session.run(
                create_relationship_query('Incidente', 'Persona', 'COINVOLGE_PERSONA', ['protocollo'], ['idpersona']),
                {'from_protocollo': incident.get('protocollo'), 'to_idpersona': idpersona_counter - 1}
            )


            # Collegamento tra Veicolo e TipoVeicolo
            session.run(
                create_relationship_query('Veicolo', 'TipoVeicolo', 'TIPO', ['protocollo', 'progressivo'], ['nome']),
                {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_nome': incident.get('tipoveicolo')}
            )

            # Collegamento tra Veicolo e Strada
            session.run(
                create_relationship_query('Veicolo', 'Strada', 'SU', ['protocollo', 'progressivo'], ['protocollo', 'nome']),
                {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_protocollo': incident.get('protocollo'), 'to_nome': incident.get('strada1')}
            )

            # Collegamento tra Veicolo e Persona (es. GUIDATO_DA)
            session.run(
                create_relationship_query('Veicolo', 'Persona', 'GUIDATO_DA', ['protocollo', 'progressivo'], ['protocollo', 'idpersona']),
                {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_protocollo': incident.get('protocollo'), 'to_idpersona': idpersona_counter - 1}
            )

# Inserisci i nodi e le relazioni
insert_data_to_neo4j(read_incidents_csv(incidents_csv))

# Chiudi la connessione al database
driver.close()
print('All data has been processed and the connection to Neo4j is closed.')
