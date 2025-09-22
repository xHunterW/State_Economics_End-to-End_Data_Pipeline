1. Navigate to the State_Economics_End-to-End_Data_Pipeline directory in the terminal.
2. Run the following command to create the Docker image and container: docker-compose up.
3. After the installation is complete, a shared folder will appear within the State_Economics_End-to-End_Data_Pipeline directory.
4. Place FinalProject.ipynb and finalprojectapi.py in the shared folder.
5. Place finalprojectpandasprofiling.py and final project cleaning & upload dag 1.py in the  State_Economics_End-to-End_Data_Pipeline/shared/airflow/dags folder
6. Create State_Economics_End-to-End_Data_Pipeline/shared/airflow/dags/data folder.
7. Place the following files in the State_Economics_End-to-End_Data_Pipeline/shared/airflow/dags/data folder: SAGDP2N__ALL_AREAS_1997_2020.csv, Minimum Wage Data.csv, nces330_20.csv, UnemploymentReport.xlsx.
8. Open a new terminal window and run this command to start the Airflow webserver: docker exec -it <container_id_or_name> airflow webserver.
9. Open another terminal window and run this command to start the Airflow scheduler: docker exec -it <container_id_or_name> airflow scheduler.
10. To access airflow, open http://localhost:8080 in the browser and use the credentials username: admin and pw: admin.
11. Run the data_cleaning_and_upload_dag DAG, which extracts, transforms, and loads the data into the PostgreSQL database. This will yield 11 total tables. Make sure that any hidden folders (.ipynb_checkpoints) are deleted. 
12. Run the generate_pandas_reports DAG to generate HTML profile reports for each table/dataset. The reports will be found in the State_Economics_End-to-End_Data_Pipeline/shared folder. 
13. Open FinalProject.ipynb and a terminal in jupyterlab. In the terminal, run the following command to start the API: python3 finalprojectapi.py.
14. You can access the data in the database by entering the desired configuration (limit and table number [0-10]) in FinalProject.ipynb. 
