# Spark ETL Job
Using PySpark to do ETL job from CSV file into PostgreSQL Table

## PostgreSQL docker
docker pull postgres

sudo docker run -d -p 5432:5432 --nam   e my-postgres -e POSTGRES_PASSWORD=postgres postgres
sudo docker exec -it my-postgres bash
psql -U postgres

### check ip container 
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' container_name_or_id

### dataset
I've scrapped data from Bukalapak.com with 15000 rows and 7 columns ('index', 'city', 'price', 'rating', 'store', 'title', 'ulasan')


