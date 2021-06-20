# Applaudo Studio ETL Test

This project written in Scala fetch data from different sources, clean and transform the data to finally store them in parquet files or show an example of the final tables on console.

## Tech
* Scala 2.12.7
* Apache Spark 2.4.6
* SBT 1.4.7

## How to use? 

It's only necessary to download the project and generate the JAR. 

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
   1. Execute with Intellij
   2. Spark local mode
   ```sh
    $ spark-submit --master local  --name ApplaudoETL --class StartETL pathToJAR/ApplaudoETL.jar --jars pathToJAR/mssql-jdbc-9.2.1.jre8.jar
    ```
   3. Spark cluster mode
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

Two tables are expected after running the code: 
* Products: Contains all the information about sold products.
```sh
+--------+-------+------------+---------+-----------------+----------------------+--------------------+--------------------+------------------+------------+
|order_id|user_id|order_number|order_dow|order_hour_of_day|days_since_prior_order|             product|              aisles|number_of_products|  department|
+--------+-------+------------+---------+-----------------+----------------------+--------------------+--------------------+------------------+------------+
| 1000867| 198377|           5|        0|               14|                     9|Triscuit Baked Wh...|            crackers|                 8|      snacks|
| 1000867| 198377|           5|        0|               14|                     9|Nutter Butter Coo...|       cookies cakes|                10|      snacks|
| 1000867| 198377|           5|        0|               14|                     9|    Chili With Beans|  canned meals beans|                 6|canned goods|
| 1000867| 198377|           5|        0|               14|                     9|Zingers Raspberry...|       cookies cakes|                 6|      snacks|
| 1000867| 198377|           5|        0|               14|                     9|        Ho Hos Cakes|       cookies cakes|                13|      snacks|
| 1000867| 198377|           5|        0|               14|                     9|Small Curd Cottag...|other creams cheeses|                14|  dairy eggs|
| 1000867| 198377|           5|        0|               14|                     9| Original Corn Chips|      chips pretzels|                14|      snacks|
| 1000867| 198377|           5|        0|               14|                     9|Original Citrus S...|         soft drinks|                13|   beverages|
|  100258| 156548|          29|        0|               23|                    15|Stage 2 Spinach, ...|   baby food formula|                12|      babies|
|  100258| 156548|          29|        0|               23|                    15| Colby Cheese Sticks|     packaged cheese|                 2|  dairy eggs|
+--------+-------+------------+---------+-----------------+----------------------+--------------------+--------------------+------------------+------------+
only showing top 10 rows
```
* Clients: Contains the Client Category and Segmentation required by Marketing and Customer Loyalty Departments

```sh
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
```
