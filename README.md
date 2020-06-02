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
I'll use titanic dataset and do little data preprocessing such as imputing missing values 


