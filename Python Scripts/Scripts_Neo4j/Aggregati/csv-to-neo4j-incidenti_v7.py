import os
import time
import csv  # Assicurarsi di avere l'import di csv
from neo4j import GraphDatabase
from tqdm import tqdm  # Importazione di tqdm per la barra di progresso

# Configurazione della connessione a Neo4j
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "adminadmin"))

# Percorsi dei file CSV degli incidenti
incidents_csv_files = [
    './Datasets/2020/csv_incidentiFebbraio.csv',  
]

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
    total_incidents = len(incidents)
    start_time = time.time()
    idpersona_counter = 1

    with driver.session() as session:
        clear_graph(session)

        # Utilizzo di tqdm per creare una barra di progresso
        with tqdm(total=total_incidents, desc="Processing incidents", unit="incident") as pbar:

            for i, incident in enumerate(incidents, start=1):
                current_time = time.time()

                # Calcolo della percentuale e tempo stimato
                percent_complete = (i / total_incidents) * 100
                elapsed_since_start = current_time - start_time
                avg_time_per_incident = elapsed_since_start / i
                remaining_incidents = total_incidents - i
                estimated_remaining_time = avg_time_per_incident * remaining_incidents

                minutes, seconds = divmod(estimated_remaining_time, 60)
                pbar.set_postfix({
                    'Progress': f"{percent_complete:.2f}%",
                    'Remaining Time': f"{int(minutes)}m {int(seconds)}s"
                })

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

                # Creazione nodo Strada in ogni caso
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

                # Creazione relazione tra Incidente e Strada
                session.run(
                    create_relationship_query('Incidente', 'Strada', 'OCCORSO_SU', ['protocollo'], ['protocollo']),
                    {'from_protocollo': incident.get('protocollo'), 'to_protocollo': incident.get('protocollo')}
                )

                # Creazione nodo Veicolo solo se tipopersona non è "pedone"
                tipopersona = incident.get('tipopersona')
                if not (tipopersona and tipopersona.lower() == 'pedone'):
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

                    # Creazione nodo Persona con idpersona autoincrementale
                    person_query, person_params = create_node_query(
                        'Persona',
                        idpersona=idpersona_counter,
                        tipolesione=incident.get('tipolesione'),
                        casco_cintura=incident.get('cinturacascoutilizzato'),
                        deceduto=incident.get('deceduto'),
                        deceduto_dopo=incident.get('deceduto_dopo'),
                        tipopersona=incident.get('tipopersona')
                    )
                    session.run(person_query, person_params)

                    # Creazione nodo Sesso
                    gender_query, gender_params = create_node_query(
                        'Sesso',
                        nome=incident.get('sesso')
                    )
                    session.run(gender_query, gender_params)

                    # Collegamento tra Persona e Sesso
                    session.run(
                        create_relationship_query('Persona', 'Sesso', 'HA_SESSO', ['idpersona'], ['nome']),
                        {'from_idpersona': idpersona_counter, 'to_nome': incident.get('sesso')}
                    )

                    # Incrementa il contatore per il prossimo idpersona
                    idpersona_counter += 1

                    # Creazione delle relazioni tra nodi
                    session.run(
                        create_relationship_query('Incidente', 'Veicolo', 'COINVOLGE_VEICOLO', ['protocollo', 'progressivo'], ['protocollo', 'progressivo']),
                        {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_protocollo': incident.get('protocollo'), 'to_progressivo': incident.get('progressivo')}
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
                        create_relationship_query('Veicolo', 'Persona', 'GUIDATO_DA', ['protocollo', 'progressivo'], ['idpersona']),
                        {'from_protocollo': incident.get('protocollo'), 'from_progressivo': incident.get('progressivo'), 'to_idpersona': idpersona_counter - 1}
                    )

                # Aggiornamento della barra di progresso
                pbar.update(1)

        print("Data insertion completed")

def main():
    incidents = read_incidents_csv(incidents_csv_files)
    insert_data_to_neo4j(incidents)

if __name__ == "__main__":
    main()
