-- Inicializacao do PostgreSQL
-- Executado automaticamente na primeira inicializacao do container.
--
-- O banco 'data_warehouse' ja e criado pela variavel POSTGRES_DB.
-- Este script cria o banco 'airflow' para os metadados do Airflow.

CREATE DATABASE airflow OWNER admin;
