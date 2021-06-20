# Applaudo Studio ETL Test

This project written in Scala fetch data from different sources, clean and transform the data to finally store them in parquet files or show an example of the final tables in console.

## Tech
* Scala 2.12.7
* Apache Spark 2.4.6
* SBT 1.4.7

## How to use? 

### Local mode
1. Clone the project:
    ```sh
    $ git clone https://github.com/carlossegovia/compactHDFSFiles.git
    ```
 2. Compile the project with SBT:
    ```sh
    $ sbt clean package
    ```
    *The JAR file will be generate in __./applaudoTest/target/scala-2.12/ApplaudoETL.jar__*

3. Execute:
    ```sh
    $ scala pathToJAR/ApplaudoETL.jar
    ```
    *Help:*

    ```sh
    Usage: ApplaudoETL.jar [-r string]
    Optional: [-r string]
    Description:
    -r: Result path.    Path to store the result tables in parquet format.
    Notes:
      If the results path is not provided, the results will be printed in console
    ```    

### Cluster mode

Assuming you have a Cluster with a functionally environment of Apache Spark, you just need to download the project and generate the JAR. 

1. Clone the project:
    ```sh
    $ git clone https://github.com/carlossegovia/compactHDFSFiles.git
    ```
 2. Compile the project with SBT:
    ```sh
    $ sbt clean package
    ```
    *The JAR file will be generate in __./applaudoTest/target/scala-2.12/ApplaudoETL.jar__*

3. Execute the JAR with Apache Spark:
    ```sh
    $ spark-submit --master yarn --deploy-mode cluster --name ApplaudoETL --class StartETL pathToJAR/ApplaudoETL.jar --jars pathToJAR/mssql-jdbc-9.2.1.jre8.jar
    ```
    *Help:*

    ```sh
    Usage: ApplaudoETL.jar [-r string]
    Optional: [-r string]
    Description:
    -r: Result path.    Path to store the result tables in parquet format.
    Notes:
      If the results path is not provided, the results will be printed in console
    ```     

## Data Studio Dashboard

A dashboard created with Google Data Studio is available [here](https://datastudio.google.com/reporting/04f80c80-eeee-4b4c-967a-14ed49b6df39), with the next charts:

* The mix of aisles with more products vs products sold
* The days of the week with the highest number of orders processed
* Number of orders per day and hour
* Client Segmentation overview (for the Marketing and Customer Loyalty Departments)

## Expected results

Two tables are expected after ran the code: 
* Products: Contains all the information about sold products.

* Clients: Contains the Client Category and Segmentation required by Marketing and Customer Loyalty Departments
+-------+------------------+-------------------------+
|user_id|category          |client_segment           |
+-------+------------------+-------------------------+
|29     |A complete mystery|Baby come Back           |
|1806   |A complete mystery|Special Offers           |
|2040   |A complete mystery|Special Offers           |
|2214   |A complete mystery|Baby come Back           |
|2927   |A complete mystery|Special Offers           |
|3764   |A complete mystery|Baby come Back           |
|5385   |A complete mystery|Special Offers           |
|6721   |A complete mystery|Undefined                |
|7225   |A complete mystery|You've Got a Friend in Me|
|7747   |A complete mystery|Baby come Back           |
+-------+------------------+-------------------------+
only showing top 10 rows
