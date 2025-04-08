# Drivers

Big Data Project A.Y. 2024/2025
- **Giovanni Antonioni** [Second Job]
- **Luca Rubboli (0001083742)** [First Job]

# Dataset

[New York City Taxi Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

All yellow, green, fhv and fhvhv datasets will be analyzed.

# First job - Anomaly detection

First job consists of the following steps:

- Dataset cleanup
- Discretization of continuous features
- Select features to categorize data in classes, then calculate average prices as price per mile ($ / miles)
- Define bins representing price slack w.r.t. average prices (e.g. avg_price | avg_price + 5 | avg_price + 10 | ...)
- Establish a subset of discrete features not used in categorization and bins
- For each feature bins, calculate the impact percentage of data points that fall into each price bin for every possible combination of values
- Results visualization through appropriate graphs

# Second job - Features impact on tips

Second job consists of the following  steps:

- Dataset cleanup
- Discretization continuous features
- Select features to categorize data in classes (average_price is calculated too and used in categorization), then calculate average tips ($)
- For each categorization, analyze the impact on tips given by the change of feature's bins, fixing all the other features, calculating the percentage change of average tips w.r.t the first bin established
- Results visualization through appropriate graphs

## Local Deployment

### Define new Run/Debug Configurations

Configuration type: Spark Submit - Local (deprecated)

Name: Spark Local

Spark home: Your spark directory

Application: point to the .jar file inside the build/libs folder of this repository; if you don't find it, build the project (`./gradlew build`)

Class: jobs.FirstJob / jobs.SecondJob

Run arguments: local

Cluster manager: Local

Master: local

It is also possible to add a before launch command, triggering gradle build task to force building before running

## Remote Deployment

Create a cluster on AWS EMR via CLI

Make sure that SSH connections are enabled on the Security Group of the master node

Under `src/main/resources`, create a file called "aws_credentials.txt"; put the value of your aws_access_key in the first line and the value of your aws_secret_access_key in the second line

Open the `src/main/scala/utils/Config` file and update the variables' values according to your settings

### Define new Run/Debug Configurations

Configuration type: Spark Submit - Cluster

Name: Spark Cluster

Region: us-east-1

Remote Target: Add EMR connection

Authentication type: Profile from credentials file

Profile name: Your profile name

Click on "Test connection" to verify: if you cannot connect or there are no deployed cluster, the connection will not be saved

Enter a new SSH Configuration

Host: the address of the primary node of the cluster, i.e., the MasterPublicDnsName

Username: hadoop

Authentication type: Key pair

Private key file: point to your .ppk / pem key

Test the connection

Application: point to the .jar file inside the build/libs folder of this repository; if you don't find it, build the project (`./gradlew build`)

Class: jobs.FirstJob / jobs.SecondJob

Run arguments: remote

Before launch: Upload Files Through SFTP

It is also possible to add a before launch command, triggering gradle build task to force building before running

## TODO
Clean up dataset (scartare dati con attributi nulli/sporchi)

Anomaly detection job

N passeggeri | fascia oraria | Trip_distance | Date[YY/YY-MM] | Trip_duration (fasce) | Fare_amount (fasce) | Total_amount (fasce) -> costo medio ($ / miles)

1 | 10-18 | 20-30 miles | 2022 | 30 - 45 minuti | 20-25 $ | 25-30$  -> 10$ / miles

 -> differenza dal costo medio

1 | Y | 1  | 100 - 125$ -> +5

RatecodeId 1 -> tot % corse prezzo medio + 5 < prezzo < prezzo medio +10
Store_and_fwd_flag Y -> tot % corse prezzo medio + 5 < prezzo < prezzo medio +10



Features bin change impact on tips

costo medio ($ / miles) (fasce) | Total_fees (Total_amount - Fare_amount - Tip_amount) (fasce) | Trip_duration (fasce) | Trip_distance (fasce) -> avg_tip

10 $ / miles | 2 - 5 $ | 30 - 40 minuti | 20 - 30 miles -> 3$
10 $ / miles | 2 - 5 $ | 30 - 40 minuti | 30 - 40 miles -> 5$


