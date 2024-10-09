import psycopg2
import csv
import chardet
from tqdm import tqdm
import os
import re 

def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="admin",
            host="localhost",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Errore nella connessione al database: {e}")
        return None

def reset_database():
    try:
        conn = connect_to_postgres()
        if conn is not None:
            conn.autocommit = True  
            cur = conn.cursor()

            cur.execute(""" 
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = 'incidenti'
                AND pid <> pg_backend_pid();
            """)
            cur.execute("DROP DATABASE IF EXISTS incidenti;")
            cur.execute("CREATE DATABASE incidenti;")
            cur.close()
            conn.close()
            print("Database incidenti cancellato e ricreato con successo.")
        else:
            print("Connessione al database fallita.")
    except Exception as e:
        print(f"Errore nella cancellazione o ricreazione del database: {e}")

def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="incidenti",
            user="postgres",
            password="admin",
            host="localhost",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Errore nella connessione al database incidenti: {e}")
        return None

def create_tables(conn):
    try:
        cur = conn.cursor()

      
        cur.execute(""" 
            CREATE TABLE IF NOT EXISTS incidente (
                Protocollo INTEGER PRIMARY KEY,
                Gruppo INTEGER,
                Dataincidente TIMESTAMP,
                chilometrica VARCHAR(60),
                natura TEXT,
                traffico VARCHAR(50),
                condizioneatm VARCHAR(50),
                visibilita VARCHAR(50),
                illuminazione VARCHAR(50),
                numero_feriti INTEGER,
                numero_illesi INTEGER,
                numero_mort INTEGER,
                longitudine DECIMAL(9,6),
                latitudine DECIMAL(9,6)
            );
        """)

        cur.execute(""" 
            CREATE TABLE IF NOT EXISTS veicolo (
                Protocollo INTEGER,
                progressivo INTEGER,
                tipo_veicolo VARCHAR(50),
                stato_veicolo VARCHAR(50),
                stato_airbag VARCHAR(50),
                PRIMARY KEY (Protocollo, progressivo),
                FOREIGN KEY (Protocollo) REFERENCES incidente(Protocollo) ON DELETE CASCADE
            );
        """)

        cur.execute(""" 
            CREATE TABLE IF NOT EXISTS strada (
                idstrada SERIAL PRIMARY KEY,
                Protocollo INTEGER,
                strada1 VARCHAR(255),
                localizzazione VARCHAR(255),
                particolarita VARCHAR(255),
                tipostrada VARCHAR(255),
                fondostradale VARCHAR(255),
                pavimentazione VARCHAR(255),
                segnaletica VARCHAR(255),
                FOREIGN KEY (Protocollo) REFERENCES incidente(Protocollo) ON DELETE CASCADE
            );
        """)

        cur.execute(""" 
            CREATE TABLE IF NOT EXISTS persona (
                idpersona SERIAL PRIMARY KEY,
                Protocollo INTEGER,
                tipopersona VARCHAR(255),
                sesso VARCHAR(255),
                tipolesione VARCHAR(255),
                cintura_casco VARCHAR(255),
                deceduto INTEGER,
                deceduto_dopo VARCHAR(255),
                FOREIGN KEY (Protocollo) REFERENCES incidente(Protocollo) ON DELETE CASCADE
            );
        """)

        conn.commit()
        cur.close()
        print("Tabelle create con successo.")
    except Exception as e:
        print(f"Errore nella creazione delle tabelle: {e}")

def insert_data_from_csv(conn, csv_files):
    try:
        cur = conn.cursor()

        for csv_file in csv_files:
            print(f"Importando dati da: {csv_file}")

            # Rilevamento della codifica del file CSV
            with open(csv_file, 'rb') as file:
                raw_data = file.read()
                result = chardet.detect(raw_data)
                encoding = result['encoding']

            try:
                with open(csv_file, 'r', encoding=encoding) as file:
                    reader = csv.DictReader(file, delimiter=';')
                    rows = list(reader)
            except Exception as e:
                print(f"Errore durante l'apertura del file {csv_file} con codifica {encoding}: {e}")
                continue

            for row in tqdm(rows, desc=f"Importando {csv_file}", unit="riga"):
                if 'Protocollo' not in row or not row['Protocollo']:
                    print("Protocollo mancante o non trovato, riga ignorata:", row)
                    continue

                cur.execute(""" 
                    INSERT INTO incidente (
                        Protocollo, Gruppo, Dataincidente, chilometrica, natura, traffico, condizioneatm, visibilita,
                        illuminazione, numero_feriti, numero_illesi, numero_mort, longitudine, latitudine
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (Protocollo) DO NOTHING;
                """, (
                    row['Protocollo'], row['Gruppo'] or None, row['DataOraIncidente'], row['Chilometrica'] or None,
                    row['NaturaIncidente'] or None, row['Traffico'] or None, row['CondizioneAtmosferica'] or None,
                    row['Visibilita'] or None, row['Illuminazione'] or None, row['NUM_FERITI'] or None,
                    row['NUM_ILLESI'] or None, row['NUM_MORTI'] or None, row['Longitude'] or None,
                    row['Latitude'] or None
                ))

                if row['Progressivo']:
                    cur.execute(""" 
                        INSERT INTO veicolo (
                            Protocollo, progressivo, tipo_veicolo, stato_veicolo, stato_airbag
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (Protocollo, progressivo) DO NOTHING;
                    """, (
                        row['Protocollo'], row['Progressivo'], row['TipoVeicolo'] or None,
                        row['StatoVeicolo'] or None, row['Airbag'] or None
                    ))

                cur.execute(""" 
                    INSERT INTO strada (
                        Protocollo, strada1, localizzazione, particolarita, tipostrada, fondostradale, pavimentazione, segnaletica
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    row['Protocollo'], row['STRADA1'] or None, row['Localizzazione1'] or None,
                    row['particolaritastrade'] or None, row['TipoStrada'] or None,
                    row['FondoStradale'] or None, row['Pavimentazione'] or None, row['Segnaletica'] or None
                ))

                cur.execute(""" 
                    INSERT INTO persona (
                        Protocollo, tipopersona, sesso, tipolesione, cintura_casco, deceduto, deceduto_dopo
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    row['Protocollo'], row['TipoPersona'] or None, row['Sesso'] or None,
                    row['Tipolesione'] or None, row['CinturaCascoUtilizzato'] or None,
                    row['Deceduto'] or None, row['DecedutoDopo'] or None
                ))

        conn.commit()
        cur.close()
        print("Dati inseriti con successo dai CSV.")
    except Exception as e:
        print(f"Errore nell'inserimento dei dati dai CSV: {e}")

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
    
    # Estrai il mese dal nome del file
    month = re.search(r'csv_incidenti(.*?).csv', filename)  
    if month:
        month_name = month.group(1).strip()  
        return month_map.get(month_name, 0)  
    return 0  

# Funzione per ottenere i file CSV ordinati per anno e mese
def get_csv_files(base_directory):
    csv_files = []
    for dirpath, _, filenames in os.walk(base_directory):
        year = os.path.basename(dirpath) 
        if year.isdigit(): 
            for filename in filenames:
                if filename.endswith('.csv'):
                    csv_files.append((year, filename)) 

    # Ordina i file CSV per anno e mese
    csv_files.sort(key=lambda x: (int(x[0]), month_order_key(x[1])))  

    # Restituisci solo i percorsi dei file ordinati
    return [os.path.join(base_directory, x[0], x[1]) for x in csv_files]  


if __name__ == "__main__":
    reset_database()  
    conn = connect_to_db()  
    if conn:
        create_tables(conn)  
        csv_files = get_csv_files("./Datasets")  
        insert_data_from_csv(conn, csv_files)  
        conn.close() 