import psycopg2
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from neo4j import GraphDatabase
import time
import re

# Connessione globale per PostgreSQL e Neo4j
postgres_conn = None
neo4j_driver = None

# Funzione di connessione a PostgreSQL
def connect_to_postgres():
    global postgres_conn
    if not postgres_conn:
        try:
            postgres_conn = psycopg2.connect(
                dbname="incidenti",
                user="postgres",
                password="admin",
                host="localhost",
                port="5432"
            )
        except Exception as e:
            print(f"Errore nella connessione a PostgreSQL: {e}")
            postgres_conn = None

# Funzione di connessione a Neo4j
def connect_to_neo4j():
    global neo4j_driver
    if not neo4j_driver:
        try:
            uri = "bolt://localhost:7687"
            neo4j_driver = GraphDatabase.driver(uri, auth=("neo4j", "adminadmin"))
        except Exception as e:
            print(f"Errore nella connessione a Neo4j: {e}")
            neo4j_driver = None

def execute_postgres_queries(queries):
    connect_to_postgres()
    if postgres_conn:
        execution_times = []
        try:
            cursor = postgres_conn.cursor()
            for query in queries:
                # Utilizza EXPLAIN per ottenere il tempo di esecuzione
                explain_query = f"EXPLAIN (ANALYZE) {query}"
                cursor.execute(explain_query)
                explain_results = cursor.fetchall()
                print("Output di EXPLAIN:", explain_results)  # Stampa l'output di EXPLAIN

                # Estrai il tempo di esecuzione dall'output
                execution_time = None
                for row in explain_results:
                    for detail in row:
                        if isinstance(detail, str):
                            # Usa regex per trovare il tempo di esecuzione
                            match = re.search(r'Execution Time:\s*([\d.]+) ms', detail)
                            if match:
                                execution_time = float(match.group(1))  # Estrai il numero e convertilo a float
                                break
                    if execution_time is not None:
                        break

                if execution_time is not None:
                    execution_times.append(execution_time)
                else:
                    print(f"Nessun tempo di esecuzione trovato per la query: {query}")

                # Esegui la query normale per ottenere i risultati
                cursor.execute(query)
                cursor.fetchall()  # Recupera i risultati

            cursor.close()
        except Exception as e:
            print(f"Errore nell'esecuzione delle query PostgreSQL: {e}")
            return None
        return execution_times
    return None

# Funzione per eseguire le query su Neo4j
def execute_neo4j_queries(queries, version):
    connect_to_neo4j()
    if neo4j_driver:
        execution_times = []
        try:
            for query in queries:
                records, summary, keys = neo4j_driver.execute_query(query, database=f"version{version}")
                execution_time = summary.result_consumed_after  # Tempo di esecuzione fornito da Neo4j
                execution_times.append(execution_time)
                print(f"Versione {version}: La query ha impiegato {execution_time} ms.")
        except Exception as e:
            print(f"Errore nell'esecuzione delle query Neo4j (Versione {version}): {e}")
            return None
        return execution_times
    return None

def close_connections():
    if postgres_conn:
        postgres_conn.close()
    if neo4j_driver:
        neo4j_driver.close()

def create_interactive_menu(postgres_times, neo4j_times, query_names):
    fig = make_subplots(rows=len(query_names), cols=1, subplot_titles=query_names, vertical_spacing=0.1)
    versions = ['PostgreSQL', 'Neo4j V1', 'Neo4j V2', 'Neo4j V3', 'Neo4j V4']
    colors = ['blue', 'green', 'red', 'purple', 'orange']

    # Verifica che ci siano tempi di esecuzione validi
    if not postgres_times or all(time is None for time in postgres_times):
        print("Errore: non ci sono tempi di esecuzione validi da PostgreSQL.")
        return

    if not neo4j_times or all(all(time is None for time in version) for version in neo4j_times):
        print("Errore: non ci sono tempi di esecuzione validi da Neo4j.")
        return

    # Calcola il massimo tempo per impostare un limite sugli assi
    max_time = max(
        max((time for time in postgres_times if time is not None), default=0),
        *(max((time for time in version if time is not None), default=0) for version in neo4j_times)
    ) * 1.1

    for i, query_name in enumerate(query_names):
        times = [postgres_times[i] if i < len(postgres_times) else 0]
        for j in range(4):
            times.append(neo4j_times[j][i] if i < len(neo4j_times[j]) else 0)

        fig.add_trace(
            go.Bar(
                x=versions, y=times, name=query_name, text=[f'{t:.2f}' for t in times],
                textposition='outside', marker_color=colors[i % len(colors)], width=0.4
            ),
            row=i + 1, col=1
        )

    fig.update_layout(
        height=300 * len(query_names), showlegend=False,
        title_text="Confronto Tempi di Esecuzione Query",
        margin=dict(l=10, r=10, t=30, b=10)
    )

    for i in range(len(query_names)):
        fig.update_yaxes(
            title_text="Tempo di esecuzione (ms)",
            row=i + 1, col=1,
            range=[0, max_time] 
        )

    fig.show()

# Definisco query
postgres_queries = [
    
    """
    SELECT i.*, v.*
    FROM incidente i
    JOIN veicolo v ON i.Protocollo = v.Protocollo
    WHERE i.Protocollo = 4733149;
    """
]

neo4j_queries = [
    
    """
    MATCH (i:Incidente {protocollo: '4733149'})-[:COINVOLGE_VEICOLO]->(v:Veicolo)
    RETURN i, v;

    """
    
    
]

# Esegui le query e ottieni i tempi
postgres_times = execute_postgres_queries(postgres_queries)
if postgres_times is None:
    print("Errore: tempi di esecuzione non acquisiti da PostgreSQL.")
    exit()

neo4j_times = []
for version in range(1, 5):
    neo4j_time = execute_neo4j_queries(neo4j_queries, version)
    if neo4j_time:
        neo4j_times.append(neo4j_time)

if not neo4j_times or len(neo4j_times) < 4:
    print("Errore: tempi di esecuzione incompleti per Neo4j.")
    exit()

# Nomi delle query
query_names = ['Query 1 (Conteggio Persone)']
# Aggiungi qui i nomi delle altre query

# Genera il grafico
create_interactive_menu(postgres_times, neo4j_times, query_names)

# Chiudi le connessioni
close_connections()
