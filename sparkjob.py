import findspark
findspark.init('/usr/local/spark/')

import findspark
from pyspark.sql import SparkSession

from pyspark.sql.types import *
from pyspark.sql.functions import isnan, when, count, col, mean
import psycopg2

def main():
    connection = psycopg2.connect(user="postgres",
                                  password="postgres",
                                  host="172.17.0.2",
                                  port="5432",
                                  database="postgres")
    cursor = connection.cursor()

    spark = initialize_Spark()

    df = loadData(spark)

    df_cleaned = preprocessing(df)

    createTable(cursor)
    connection.commit()

    insert_query, titanic = insertData(df_cleaned)

    cursor.execute(insert_query, titanic)

    print("Data inserted into PostgreSQL", "\n")

    selectData(cursor)

    cursor.close()


    print("Commiting changes to database", "\n")
    connection.commit()

    print("Closing connection", "\n")

    # close the connection
    connection.close()

    print("Done!", "\n")

def initialize_Spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Titanic ETL Job") \
        .getOrCreate()

    return spark

def loadData(spark):
    
    df = spark.read.csv('titanic.csv',header=True,inferSchema=True)
    print("Data loaded into PySpark", "\n")

    return df

def preprocessing(df):
    mean_age = df.select(mean(col('Age'))).collect()[0][0]
    
    # impute missing age
    df = df.withColumn('Age', when(col('Age').isNull(), mean_age). otherwise(df['Age']) )
    
    #input missing embarked
    df = df.withColumn('Embarked', when(col('Embarked').isNull(), 'S'). otherwise(df['Embarked']) )

    #drop cabin
    df = df.drop('Cabin','Name')

    return df

def createTable(cursor):
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS titanic \
    (   PassengerId integer NOT NULL, \
        Pclass integer NOT NULL, \
        Survived integer NOT NULL, \
        Sex VARCHAR(255), \
        Age integer NOT NULL, \
        SibSp integer NOT NULL, \
        Parch integer NOT NULL, \
        Ticket VARCHAR(255) NOT NULL, \
        Fare float NOT NULL, \
        Embarked VARCHAR(255));")

        print("Table successfully created", "\n")
    except:
        print("Error creating Titanic table", "\n")

def insertData(df):

    titanic = [tuple(x) for x in df.collect()]

    print(titanic[1])
    template = ",".join(["%s"] * len(titanic))

    insert_query = "INSERT INTO titanic (passengerId, pclass, survived, sex, age, sibSp, \
                        parch, ticket, fare, embarked \
                           ) VALUES {}".format(template)

    print("Data Inserted", "\n")

    return insert_query, titanic

def selectData(cursor):
    query = "select passengerId, Sex, Age, Survived from titanic"

    cursor.execute(query)

    records = cursor.fetchmany(5)

    for row in records:
        print("passengerId = ", row[0])
        print("Sex = ", row[1])
        print("Age = ", row[2])
        print("Survide  = ", row[3], "\n")


if __name__ == '__main__':
    main()