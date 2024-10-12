from neo4j import GraphDatabase
import csv
import os

print("Current working directory:", os.getcwd())

# Configura la connessione a Neo4j
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "adminadmin"))
db_name = "Neo4j"  # Nome del tuo database

# Percorso del file CSV degli incidenti
incidents_csv = './csv_incidentiFebbraio.csv'  # Modifica con il percorso del tuo CSV

def create_incident_query(labels, **kwargs):
    # Filtra i parametri per rimuovere quelli vuoti
    filtered_kwargs = {key: value for key, value in kwargs.items() if value}
    param_str = ', '.join([f'{key}: ${key}' for key in filtered_kwargs.keys()])
    return f"CREATE (n:{labels} {{ {param_str} }})", filtered_kwargs

# Lista per gli incidenti
incidents = []

# Leggi il file CSV degli incidenti
try:
    with open(incidents_csv, mode='r', encoding='latin-1') as incidents_file:
        reader = csv.DictReader(incidents_file, delimiter=';')  # Specifica il delimitatore
        for row in reader:
            # Pulisci le chiavi e valori del dizionario
            cleaned_row = {key.strip().replace(' ', '_'): value for key, value in row.items()}
            incidents.append(cleaned_row)
except Exception as e:
    print(f"An error occurred while reading the file: {e}")
    exit(1)

print('Data extraction complete\n')

# Crea i nodi in Neo4j
try:
    with driver.session(database=db_name) as session:
        print('Creating incident nodes...')
        for incident in incidents:
            query, params = create_incident_query('Incidente', **incident)
            session.run(query, **params)
        print('{} incident nodes successfully created\n'.format(len(incidents)))
except Exception as e:
    print(f"An error occurred while creating nodes in Neo4j: {e}")
finally:
    driver.close()
    print('Successfully created {} nodes in total'.format(len(incidents)))
