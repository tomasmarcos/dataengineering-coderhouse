#  ETL with airflow and smtp
ETL that retrieves data from an unnoficial api that contains
statistics of the argentinian central bank.

## To check if it works:
    - See the screenshots from the folder "proof_it_works". 

## To Run this ETL:
    1) Get your api credentials from this website: https://estadisticasbcra.com/api/registracion  and your own redshift credentials. Also include your smtp credentials. See sample_envfile to check how you should define it.
    2) Copy everything from step 1 into an .env file.  Check project_dag.py to see which names of the env files you should use. This env file will be exported.
    3) Make sure you have project_dag.py ; requirements.txt ; Dockerfile ; scripts; .env file in the same folder as  docker-compose.yaml
    4) Install airflow through  docker-compose.yaml . This custom .yaml runs a custom dockerfile that will copy project_dag.py as well as scripts folder. For this just run docker compose up airflow-init (make sure you have docker installed and your container is running)
    5) To start it just run docker compose up inside your airflow folder (where you placed your docker-compose.yaml and everything).
    6) Go to localhost:8080 and enable the  etl ; it is scheduled on a daily basis.
