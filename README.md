# Experiment to do ETL with PySpark from CSV file into PostgreSQL table

## PostgreSQL docker
docker pull postgres

sudo docker run -d -p 5432:5432 --name my-postgres -e POSTGRES_PASSWORD=postgres postgres
sudo docker exec -it my-postgres bash
psql -U postgres

### check ip container 
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' container_name_or_id
