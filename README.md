
This DAG fetches the top 3 locations from an API and loads them into a PostgreSQL database. The DAG will run daily according to the specified schedule.
This project contains an Apache Airflow DAG (Directed Acyclic Graph) that performs the following actions:

1. Defining constants and settings:
   - Sets the basic arguments (default_args) for the DAG, such as the owner, dependencies on previous runs, start date, and execution interval.
   - Sets the URL for retrieving data from an API.

2. Defining the `load_to_db` function:
   - This function is responsible for loading data into a database.
   - It receives values passed through `kwargs`.
   - Retrieves the list of locations (locations_list) from the XCom returned by the previous task, "get_top3_locations".
   - Establishes a connection to a PostgreSQL database using the PostgresHook.
   - Creates the "s_nikulina_20_ram_location" table in the database if it doesn't exist.
   - Truncates the "s_nikulina_20_ram_location" table.
   - Loads the data from the locations list into the table.
   - Commits the changes to the database and logs the number of inserted rows.

3. Defining the "s-nikulina-20_lesson_5" DAG:
   - Specifies the name of the DAG, the execution schedule, default arguments, and other settings.
   - Sets the tag "s_nikulina-20" to identify the DAG.
   - Defines two tasks within the DAG:
     - "get_top3_locations": Uses the custom operator SNikulinaRamMortyLocationsOperator to retrieve the top 3 locations from an API.
     - "load_locations_to_db": Uses the PythonOperator, which invokes the `load_to_db` function to load the data into a database.
   - Sets the dependency between the tasks, where "get_top3_locations" is executed before "load_locations_to_db".


