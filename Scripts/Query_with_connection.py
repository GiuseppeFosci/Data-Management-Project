import psycopg2
import matplotlib.pyplot as plt
import numpy as np
from neo4j import GraphDatabase
import time
import re

postgres_conn = None
neo4j_driver = None

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

def connect_to_neo4j():
    global neo4j_driver
    if not neo4j_driver:
        try:
            uri = "bolt://localhost:7687"
            neo4j_driver = GraphDatabase.driver(uri, auth=("neo4j", "adminadmin"))
        except Exception as e:
            print(f"Errore nella connessione a Neo4j: {e}")
            neo4j_driver = None

def execute_postgres_queries_with_analyze(queries):
    connect_to_postgres()
    if postgres_conn:
        execution_times = []
        try:
            cursor = postgres_conn.cursor()
            for query in queries:
                explain_query = f"EXPLAIN (ANALYZE) {query}"
                cursor.execute(explain_query)
                explain_results = cursor.fetchall()
                print("Output di EXPLAIN:", explain_results)  

                execution_time = None
                for row in explain_results:
                    for detail in row:
                        if isinstance(detail, str):
                            match = re.search(r'Execution Time:\s*([\d.]+) ms', detail)
                            if match:
                                execution_time = float(match.group(1))  
                                break
                    if execution_time is not None:
                        break

                if execution_time is not None:
                    execution_times.append(execution_time)
                else:
                    print(f"Nessun tempo di esecuzione trovato per la query: {query}")

                cursor.execute(query)
                cursor.fetchall()  

            cursor.close()
        except Exception as e:
            print(f"Errore nell'esecuzione delle query PostgreSQL: {e}")
            return None
        return execution_times
    return None


def execute_postgres_queries(queries):
    connect_to_postgres()
    if postgres_conn:
        execution_times = []
        try:
            cursor = postgres_conn.cursor()
            for query in queries:
                start_time = time.time()
                
                cursor.execute(query)
                cursor.fetchall()  
                
                end_time = time.time()
                total_execution_time = (end_time - start_time) * 1000  
                
                execution_times.append(total_execution_time)
                print(f"Tempo totale di esecuzione (query completa): {total_execution_time:.2f} ms")
                
            cursor.close()
        except Exception as e:
            print(f"Errore nell'esecuzione delle query PostgreSQL: {e}")
            return None
        return execution_times
    return None


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
    versions = ['PostgreSQL', 'Neo4j V1', 'Neo4j V2', 'Neo4j V3', 'Neo4j V4']
    colors = ['blue', 'green', 'red', 'purple', 'orange']

    if not postgres_times or all(time is None for time in postgres_times):
        print("Errore: non ci sono tempi di esecuzione validi da PostgreSQL.")
        return

    if not neo4j_times or all(all(time is None for time in version) for version in neo4j_times):
        print("Errore: non ci sono tempi di esecuzione validi da Neo4j.")
        return

    max_time = max(
        max((time for time in postgres_times if time is not None), default=0),
        *(max((time for time in version if time is not None), default=0) for version in neo4j_times)
    ) * 1.1

    for i, query_name in enumerate(query_names):
        times = [postgres_times[i] if i < len(postgres_times) else 0]
        for j in range(4):
            times.append(neo4j_times[j][i] if i < len(neo4j_times[j]) else 0)

        plt.figure(figsize=(8, 6))
        plt.bar(versions, times, color=colors[:len(times)])
        plt.title(f"Tempo di esecuzione per la query: {query_name}")
        plt.ylabel("Tempo di esecuzione (ms)")
        plt.ylim(0, max_time)
        
        for j, time in enumerate(times):
            plt.text(j, time + 5, f'{time:.2f}', ha='center', va='bottom')

        plt.tight_layout()
        plt.show()

postgres_queries = [
    ##### QUERY 1 ######
    """ 
    SELECT *
    FROM Incidente
    """,

    ##### QUERY 2 ######
    """
    SELECT i.protocollo
    FROM Incidente i 
    JOIN veicolo v ON i.protocollo = v.protocollo
    """,

    ##### QUERY 3 ######
    """
    SELECT gruppo, COUNT(*) AS numero_incidenti
    FROM incidente
    GROUP BY gruppo
    ORDER BY gruppo;
    """,

    ### QUERY 4 ####
    """
    SELECT v.*, i.*, s.*
    FROM Veicolo v
    JOIN Incidente i ON i.protocollo = v.protocollo
    JOIN Strada s ON s.protocollo = i.protocollo
    WHERE v.tipo_veicolo = 'Velocipede';
    """,

    ##### QUERY 5 ######
    """
    SELECT i.protocollo, p1.idpersona, v.tipo_veicolo
    FROM incidente AS i
    JOIN veicolo AS v ON i.protocollo = v.protocollo
    JOIN persona AS p1 ON i.protocollo = p1.protocollo
    WHERE v.tipo_veicolo = 'Autovettura privata'

    UNION

    SELECT i.protocollo, p2.idpersona, v.tipo_veicolo
    FROM incidente AS i
    JOIN veicolo AS v ON i.protocollo = v.protocollo
    JOIN persona AS p1 ON i.protocollo = p1.protocollo
    JOIN persona AS p2 ON p1.protocollo = p2.protocollo AND p1.idpersona <> p2.idpersona
    WHERE v.tipo_veicolo = 'Autovettura privata'

    UNION

    SELECT i.protocollo, p3.idpersona, v.tipo_veicolo
    FROM incidente AS i
    JOIN veicolo AS v ON i.protocollo = v.protocollo
    JOIN persona AS p1 ON i.protocollo = p1.protocollo
    JOIN persona AS p2 ON p1.protocollo = p2.protocollo AND p1.idpersona <> p2.idpersona
    JOIN persona AS p3 ON p2.protocollo = p3.protocollo AND p2.idpersona <> p3.idpersona
    WHERE v.tipo_veicolo = 'Autovettura privata'

    ORDER BY protocollo;
    """,

    #### QUERY 6 ####
    """
    SELECT i.protocollo, p1.idpersona, v.tipo_veicolo, s.strada1 AS nome_strada
    FROM incidente AS i
    JOIN veicolo AS v ON i.protocollo = v.protocollo
    JOIN persona AS p1 ON i.protocollo = p1.protocollo
    JOIN strada AS s ON i.protocollo = s.protocollo
    WHERE v.tipo_veicolo = 'Autovettura privata'

    UNION

    SELECT i.protocollo, p2.idpersona, v.tipo_veicolo, s.strada1 AS nome_strada
    FROM incidente AS i
    JOIN veicolo AS v ON i.protocollo = v.protocollo
    JOIN persona AS p1 ON i.protocollo = p1.protocollo
    JOIN persona AS p2 ON p1.protocollo = p2.protocollo AND p1.idpersona <> p2.idpersona
    JOIN strada AS s ON i.protocollo = s.protocollo
    WHERE v.tipo_veicolo = 'Autovettura privata'

    UNION

    SELECT i.protocollo, p3.idpersona, v.tipo_veicolo, s.strada1 AS nome_strada
    FROM incidente AS i
    JOIN veicolo AS v ON i.protocollo = v.protocollo
    JOIN persona AS p1 ON i.protocollo = p1.protocollo
    JOIN persona AS p2 ON p1.protocollo = p2.protocollo AND p1.idpersona <> p2.idpersona
    JOIN persona AS p3 ON p2.protocollo = p3.protocollo AND p2.idpersona <> p3.idpersona
    JOIN strada AS s ON i.protocollo = s.protocollo
    WHERE v.tipo_veicolo = 'Autovettura privata'

    ORDER BY protocollo;
    """,

    ### QUERY 7 ####
    """
    SELECT 
    i.protocollo,
    p.idpersona,
    p.tipolesione,
    v1.tipo_veicolo AS tipoVeicolo_v1,
    v2.tipo_veicolo AS tipoVeicolo_v2
    FROM 
    incidente AS i
    JOIN 
    persona AS p ON i.protocollo = p.protocollo AND p.sesso = 'M' AND p.tipolesione = 'Prognosi riservata'
    JOIN 
    veicolo AS v1 ON i.protocollo = v1.protocollo
    JOIN 
    veicolo AS v2 ON i.protocollo = v2.protocollo
    JOIN 
    strada AS s ON v1.protocollo = s.protocollo AND s.fondostradale = 'Asciutto'
    WHERE 
    v1.tipo_veicolo <> v2.tipo_veicolo  
    ORDER BY i.protocollo;
    """,

    #QUERY 8
    """
    SELECT i.*, v.*, s.*
    FROM incidente AS i
    JOIN veicolo AS v ON i.protocollo = v.protocollo
    JOIN strada AS s ON v.protocollo = s.protocollo
    WHERE i.protocollo = '4733221';
    """,

    ###QUERY 9 
    """
    SELECT i.*, v.*, s.*
    FROM incidente AS i
    JOIN veicolo AS v ON i.protocollo = v.protocollo
    JOIN strada AS s ON v.protocollo = s.protocollo;
    """,

    ### QUERY 10
    """
    SELECT i.*, s.*, v.*
    FROM incidente AS i
    JOIN strada AS s ON i.protocollo = s.protocollo
    JOIN veicolo AS v ON i.protocollo = v.protocollo
    WHERE i.gruppo = '26';
    """
]


neo4j_queries = [
    """
    MATCH(i:Incidente)
    RETURN i
    """,
    """
    MATCH(i:Incidente)-[:COINVOLGE_VEICOLO]->(v:Veicolo)
    RETURN i,v
    """,
    """
    MATCH (i:Incidente)
    WITH toInteger(i.gruppo) AS gruppo, COUNT(i) AS numero_incidenti
    RETURN gruppo, numero_incidenti
    ORDER BY gruppo;
    """,
    #MATCH (i:Incidente)<-[:INTERVENUTO]-(g:Gruppo)
    #WITH g, COUNT(i) AS numero_incidenti
    #RETURN g.nome AS gruppo, numero_incidenti
    #ORDER BY toInteger(g.nome);
    

    
    ##### QUERY 4 V1######
    """
    MATCH (v:Veicolo {tipoveicolo: "Velocipede"})<-[:COINVOLGE_VEICOLO]-(i:Incidente)-[:OCCORSO_SU]->(s:Strada)
    RETURN v, i, s
    """,
    ##### QUERY 4 V2,V3 #####
    """
    MATCH (tv:TipoVeicolo {nome: "Velocipede"})<-[:TIPO]-(v:Veicolo)<-[:COINVOLGE_VEICOLO]-(i:Incidente)-[:OCCORSO_SU]->(s:Strada)
    RETURN v, i, s
    """,
    ##### QUERY 4 V4 ######
    """
    MATCH (tv:TipoVeicolo {nome: "Velocipede"})-[:TIPO]->(v:Veicolo)-[:VEICOLO_COINVOLTO_IN]->(i:Incidente)-[:OCCORSO_SU]->(s:Strada)
    RETURN v, i, s
    """,
    

    #### Query 5 - version 1
    #MATCH (i:Incidente)-[:COINVOLGE_VEICOLO]->(v:Veicolo)
    #WHERE v.tipoveicolo = "Autovettura privata"
    #MATCH (i)-[:COINVOLGE_PERSONA*1..3]->(p:Persona)
    #RETURN DISTINCT i.protocollo, p.idpersona, v.tipoveicolo AS tipoVeicolo
    #ORDER BY i.protocollo

    #### Query 5 - Version 2-3-4
    """
    MATCH (i:Incidente)-[:COINVOLGE_VEICOLO]->(v:Veicolo)-[:TIPO]->(t:TipoVeicolo {nome: "Autovettura privata"}),
      (i)-[:COINVOLGE_PERSONA*1..3]->(p:Persona)
    RETURN DISTINCT i.protocollo, p.idpersona, t.nome
    ORDER BY i.protocollo

    """,
    ###Query 6 -  Version 1
    """
    MATCH (i:Incidente)-[:COINVOLGE_VEICOLO]->(v:Veicolo)
    WHERE v.tipoveicolo = "Autovettura privata"
    MATCH (i)-[:COINVOLGE_PERSONA*1..3]->(p:Persona)
    MATCH (i)-[:OCCORSO_SU]->(s:Strada)  // Aggiunta del join per la strada
    RETURN i.protocollo, 
       p.idpersona, 
       v.tipoveicolo AS tipoVeicolo, 
       s.nome AS nomeStrada  // Restituzione del nome della strada
    ORDER BY i.protocollo;

    
    """,
    ### Query 6 -Version 2,3,4
    
    #MATCH (i:Incidente)-[:COINVOLGE_VEICOLO]->(v:Veicolo)-[:TIPO]->(t:TipoVeicolo {nome: "Autovettura privata"}),
    #  (i)-[:COINVOLGE_PERSONA*1..3]->(p:Persona),
    #  (i)-[:OCCORSO_SU]->(s:Strada)  // Aggiunta del join per la strada
    #RETURN i.protocollo, 
    #   p.idpersona, 
    #   t.nome AS tipo_veicolo, 
    #   s.nome AS nome_strada
    #ORDER BY i.protocollo;



    ### Query 7 Version 1 
    """
    MATCH (i:Incidente)-[:COINVOLGE_PERSONA]->(p:Persona {sesso: 'M', tipolesione: 'Prognosi riservata'}),
    (i)-[:COINVOLGE_VEICOLO]->(v1:Veicolo)-[:SU]->(s:Strada {fondostradale: 'Asciutto'}),
    (i)-[:COINVOLGE_VEICOLO]->(v2:Veicolo)-[:SU]->(s)
    WHERE v1.tipoveicolo <> v2.tipoveicolo  
    RETURN i.protocollo, p.idpersona, p.tipolesione, v1.tipoveicolo, v2.tipoveicolo
    """,
    ### QUery 7 - Version 2 3 4
    #MATCH (i:Incidente)-[:COINVOLGE_PERSONA]->(p:Persona {sesso: 'M', tipolesione: 'Prognosi riservata'}),
    #  (i)-[:COINVOLGE_VEICOLO]->(v1:Veicolo)-[:TIPO]->(t1:TipoVeicolo), // Collega v1 a TipoVeicolo
    #  (i)-[:COINVOLGE_VEICOLO]->(v2:Veicolo)-[:TIPO]->(t2:TipoVeicolo), // Collega v2 a TipoVeicolo
    #  (v1)-[:SU]->(s:Strada {fondostradale: 'Asciutto'}),
    #  (v2)-[:SU]->(s)
    #WHERE t1.nome <> t2.nome  // Assicurati che i tipi di veicolo siano diversi
    #RETURN i.protocollo, p.idpersona, p.tipolesione, t1.nome AS tipoVeicolo_v1, t2.nome AS tipoVeicolo_v2

    ### QUERY 8 ###
    ### Molto piu efficiente su neo4j, ciò è dovuto al fatto che parte da un nodo specifico
    """
    MATCH (i:Incidente {protocollo: '4733221'})-[:COINVOLGE_VEICOLO]->(v:Veicolo)-[:SU]->(s:Strada)
    RETURN i,v,s
    """,
    ### QUERY 9 
    ### Meno efficiente du neo4j, ciò è dovuto dal fatto che non parte da un nodo specifico
    """
    MATCH (i:Incidente)-[:COINVOLGE_VEICOLO]->(v:Veicolo),(v)-[:SU]->(s:Strada)
        RETURN i, v, s;
    """,
    
    
    #QUERY 10 VERSIONE 3-4. Meglio su neo4j sto partendo da un nodo specifico per l'attributo gruppo e poi navigo le relazioni tra nodi,
    #in questa versione dove posso sfruttare il nodo gruppo ttengo performance migliori rispetto alle versioni 1 e 2 dove il gruppo è un attributo di incidente 
    """
    MATCH (g:Gruppo {nome: "26"})-[:INTERVENUTO]->(i:Incidente)-[:OCCORSO_SU]->(s:Strada),
        (i)-[:COINVOLGE_VEICOLO]->(v:Veicolo)
        RETURN i, s, v;
    """,
    #QUERY 10 VERSIONE 1-2. Ottengo performance superiori rispetto a sql ma peggiori rispetto alla query dove posso sfruttare gruppo come nodo e navigare le relazioni.
    # MATCH (i:Incidente{gruppo: '26'})-[:COINVOLGE_VEICOLO]->(v:Veicolo),(v)-[:SU]->(s:Strada)
    #    RETURN i, v, s;
    

]

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

query_names = [
    'Query 1 (Informazioni di tuti gli incidenti)', 
    'Query 2 (Visualizza gli incidenti ed i veicoli coinvolti)',
    'Query 3 (Conteggio numero incidenti per dato gruppo)',
    'Query 4 (Visualizzo infomazioni su incidenti occorsi su strada che coinvolgo un tipo specifico di veicolo)',
    'Query 5 (Identificare tutti gli incidenti che coinvolgono un autovettura privata e di trovare tutte le persone coinvolte in incidenti correlati fino a una profondità di 3.)',
    'Query 6 (Informazioni dettagliate su incidenti stradali specifici)',
    'Query 7 (Cerca incidenti specifici in cui sono coinvolti uomini con lesione tipolesione = Prognosi riservata e ci sono almeno due veicoli diversi coinvolti nello stesso incidente in cui la strada ha un fondo Asciutto)',
    'Query 8 (Informazioni incidenti partendo da un nodo specifico)',
    'Query 9 (Informazioni incidenti partendo da nodo generico)',
    'Query 10 (Informazioni su incidenti dove è intervenuto il gruppo 26)'

]  

create_interactive_menu(postgres_times, neo4j_times, query_names)

close_connections()
