-- Criado na 1ª subida do volume pgdata (docker-entrypoint-initdb.d).
-- Garante o banco de metadados do Airflow além do banco serving (finance).
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
