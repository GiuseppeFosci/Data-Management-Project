# Road Incident Data Management Project - AA 2023/2024 Cardinali - Fosci


A Comparative Analysis of Efficiency and Simplicity in Managing Complex Data: Graph vs SQL. 

In this repository we have three Python scripts that help import, process, and modeling a csv dataset into neo4j.

# Project Description
This project aims to manage a dataset containing road incidents by creating a graph in Neo4j that represents the incidents, the people involved, vehicles, and roads. The main objective is to connect various subgraphs to form a larger and more comprehensive graph, enabling better data analysis and visualization.

## Features

- **Data Import**: Import data from CSV files containing information on road incidents.
- **Node Creation**: Create nodes for incidents, involved people, vehicles, vehicle types, and roads.
- **Relationships**: Establish relationships between nodes to represent connections between incidents, vehicles, people, and roads.
- **Graph Cleanup**: Option to delete all nodes and relationships in the graph before each import.
## Project Structure

## Project Structure

### Scripts_Neo4j
- `v1.py`: Initial version of the script that creates nodes and relationships for incidents, vehicles, and persons in Neo4j but lacks connectivity between different incidents.
- `v2.py`: This version enhances connectivity in Neo4j by making `TipoVeicolo` a separate node. This allows for a better connection between the various subgraphs created in the first version, resulting in a larger, interconnected graph.
- `v3.py`: Further increases connectivity in Neo4j by introducing `Gender` as a new node, allowing for more complex relationships and analyses related to gender in the incident data.

### Scripts_PgAdmin
- `v1.py`: A script that imports the CSV dataset into PostgreSQL, automatically loading all CSV files in the specified directory, creating tables, and inserting data while maintaining relationships through foreign keys.

## Requirements

- Python 3.x
- Neo4j Database
- Required Python libraries:
  - `neo4j`
  - `psycopg2`
  - `os` #For 
  - `re`
  - `csv`
  - `chardet`  # For detecting CSV file encoding
 
## Configuration for Postgres

1. **Modify the following section of the code in v1.py to configure the connection to PostgreSQL with your credentials:

   ```python
   import psycopg2

    conn = psycopg2.connect(
      dbname="your_database_name",
      user="your_username",
      password="your_password",
      host="localhost",
      port="5432"
)
   ```
 
## Configuration for Neo4j

1. **Connect to Neo4j**: Modify the following section of the code to configure the connection to Neo4j with your credentials:

   ```python
   uri = "bolt://localhost:7687"
   driver = GraphDatabase.driver(uri, auth=("neo4j", "adminadmin"))
   ```

