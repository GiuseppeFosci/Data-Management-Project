import subprocess
import inquirer
from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "adminadmin"))

# Mappatura degli script ai database
script_to_db = {
    'Scripts/Scripts_PgAdmin/v1.py': None,
    'Scripts/Scripts_Neo4j/csv-to-neo4j-incidenti_v1.py': 'version1',
    'Scripts/Scripts_Neo4j/csv-to-neo4j-incidenti_v2.py': 'version2',
    'Scripts/Scripts_Neo4j/csv-to-neo4j-incidenti_v3.py': 'version3',
    'Scripts/Scripts_Neo4j/csv-to-neo4j-incidenti_v4.py': 'version4'
}

def run_script(script):
    print(f"Esecuzione dello script: {script}")
    subprocess.run(['python', script])

def clear_and_create_database(db_name):
    if db_name:
        with driver.session() as session:
           
            try:
                session.run(f"DROP DATABASE {db_name}")
                print(f"Database '{db_name}' cancellato.")
            except Exception as e:
                print(f"Errore durante la cancellazione del database '{db_name}': {e}")
            
            session.run(f"CREATE DATABASE {db_name}")
            print(f"Database '{db_name}' ricreato.")

if __name__ == "__main__":
    scripts = list(script_to_db.keys())
    scripts.append('Esegui tutti gli script')

    questions = [
        inquirer.Checkbox('selected_scripts',
                          message="Seleziona gli script da eseguire",
                          choices=scripts,
                          ),
    ]

    while True:
        answers = inquirer.prompt(questions)
        
        if not answers['selected_scripts']:
            print("Nessuno script selezionato.")
            continue

        for script in answers['selected_scripts']:
            db_name = script_to_db.get(script)
            clear_and_create_database(db_name)

        if 'Esegui tutti gli script' in answers['selected_scripts']:
            print("Esecuzione di tutti gli script...")
            for script in scripts[:-1]:
                run_script(script)
        else:
            for script in answers['selected_scripts']:
                run_script(script)

        run_more = inquirer.confirm("Vuoi eseguire altri script?", default=True)
        if not run_more:
            print("Uscita dal programma.")
            break

    driver.close()
