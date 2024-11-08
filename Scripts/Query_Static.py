import matplotlib.pyplot as plt
import os

def input_execution_times(query_name):
    execution_time = None
    print(f"Nota: digita 'exit' per interrompere l'inserimento dei tempi per {query_name}.")
    
    while True:
        time_input = input(f"Inserisci il tempo di esecuzione per {query_name} (in ms): ")
        
        
        if time_input.lower() == 'exit':
            print("Inserimento dei tempi interrotto.")
            return None  
            
        try:
            execution_time = float(time_input)
            return execution_time 
        except ValueError:
            print("Input non valido, per favore inserisci un numero o 'exit'.")

def create_bar_chart(postgres_time, neo4j_times, query_name, save_dir):
    versions = ['PostgreSQL', 'Neo4j V1', 'Neo4j V2', 'Neo4j V3', 'Neo4j V4']
    times = [postgres_time] + neo4j_times

    plt.figure(figsize=(10, 6))
    bar_width = 0.3  
    x = range(len(versions))  

    plt.bar(x, times, width=bar_width, color=['blue', 'green', 'red', 'purple', 'orange'])

    plt.xticks(x, versions)
    
    plt.title(f"Tempo di Esecuzione - {query_name}")
    plt.ylabel("Tempo di esecuzione (ms)")
    
    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, f"{query_name.replace(' ', '_')}.png"))
    plt.close()

save_directory = "grafici"
os.makedirs(save_directory, exist_ok=True) 
print(f"La cartella '{save_directory}' è stata creata o esiste già.")

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

def insert_times_and_generate_chart():
    print("Scegli per quale query vuoi inserire i tempi e generare il grafico (1-10):")
    for i, query_name in enumerate(query_names, start=1):
        print(f"{i}. {query_name}")

    query_choice = int(input("Numero della query: ")) - 1

    if 0 <= query_choice < len(query_names):
        query_name = query_names[query_choice]
        
        postgres_time = input_execution_times(f"PostgreSQL {query_name}")
        
        neo4j_times = []
        for version in range(1, 5):
            neo4j_time = input_execution_times(f"Neo4j V{version} {query_name}")
            neo4j_times.append(neo4j_time)
        
        create_bar_chart(postgres_time, neo4j_times, query_name, save_directory)
        print(f"Grafico per {query_name} salvato in '{save_directory}'.")
    else:
        print("Scelta non valida. Per favore inserisci un numero tra 1 e 10.")

insert_times_and_generate_chart()