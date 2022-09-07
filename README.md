# Capstone Project for Udacity Data Engineering Nanodegree
## Purpose

In 2016, the brazilian federal government enacted the nº 8.777/2016 degree, which is known as Federal Government Open Data Policy. Since then, significant amount of governemnt agencies started to made available different data sources for public knowledge in it. This policy was only possible due to the 209 Supplementary Law 131 - the update of brazilian Fiscal Transparency Law, that dictates that every public expense must be published.

This project tracks two different sources of government expenses data, both of them made available in Government Transparency Expenses Portal (Portal da Transparência).

The first data source is Taxigov app. It was developed for brazilian government employees request taxi rides to attend external event. This data is updated daily in its source and consists in a zipped csv file.

The second data source is Brazilian Treasury Department Siconfi API. This source contains all public expenses data, its costs center, all public bids related to it and its updated daily in its source. 

Due to the enormous volume of data available in Siconfi API, this project limited the government agencies to the Treasury Department itself (usg=235876). To do that, a batch of data from 2021 and 2022 was previously processed and loaded to S3 bucket. From that, the `public_expenses_request_dag` will request data daily for the current reference month.

## Tools and Technology 

For orchestration, the Airflow framework was chose. It provides a robust and customizable solution for our data pipeline. Also, this project provides a `docker-compose.yaml` file that makes it easier to develop, test and deploy the data pipeline.

This project was developed for AWS Cloud, storing raw requested data in S3 and processing it in Redshift Clusters. The AWS Cloud was chose since it provides a variety of integrated services and products and the development team is familiar to its structure. It worked well with the current volume of data. Also, even if the volume of data increases 100x, we could easily migrate the processing solution to Spark EMR. 

## Data Access

Since this project was developed for AWS Cloud, the access control of data is managed by the IAM users, roles and policies framework.

If it is necessary to made the data avaiable for a greater number of people, we could easily managed that by IAM and create more users with certain Redshift access roles and policies.

## Data Update

The data is updated daily in its source and both pipelines request it daily. As Airflow was the solution for data orchestration, we could easily edit the schedule interval, if necessary, of both request and process dags to made data avaiable and updated at 7am.

## Data Sources

### Taxigov
The Taxigov data is available in the following ZIP file: http://repositorio.dados.gov.br/seges/taxigov/taxigov-corridas-completo.zip

### Public Expenses
The public expenses data is available in Siconfi API: https://apidatalake.tesouro.gov.br/ords/custos/tt/demais

The request parameters must be passed through URL search parameters. So, as an example, if we want to request 2022 data for Treasury Department expanses, the requested URL sould be:
https://apidatalake.tesouro.gov.br/ords/custos/tt/demais?organizacao=235876&ano=2022

## How to execute this Data Pipeline

To execute Airflow locally, we have to start the containers created in `docker-compose.yaml` file. But, first, we must define `AIRFLOW_UID` and `AIRFLOW_GID` variables in a `.env` file.

To do that, move to the local project repository folder and execute:
```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

After that, start the `airflow-init` bash script within `docker-compose.yaml` file:
```
docker-compose up airflow-init
```

Now, you can start the containers:
```
docker-compose up
```

After Airflow started locally, we must configure the Varaibles and Connections for our installations. For this project, the following Variables must be passed:

```
expenses_s3_bucket = brazilian-politicians-expenses-data
taxigov_s3_bucket = brazilian-politicians-taxi-rides
taxigov_csv_s3_key = taxigov_corridas.csv
```

For the Connections, two connections must be created: `aws_credentials` (Connection Type: `Amazon Web Services`) and `redshift_default` (Connection Type: `Amazon Redshift`).

## DAGS

There are four dags in this project: 

1. `public_expenses_request_data`:
    
    - Request data from Siconfi API for Treasury Department in the current `month` and `year`;
    - Stores the JSON response data in S3 (the JSON format is ready for Redshift `json 'auto'` copy options);
    - Creates `raw__public_expenses_data` Redshift table, if the table already not exists;
    - Populate `raw__public_expenses_data` Redshift table, via UPSERT for the unique keys `co_situacao_icc`, `me_referencia`, `an_referencia` and `co_natureza_despesa_deta`) with the requested data.

<br/>

2. `taxigov_request_dag`: 

    - Request data from Taxigov rides data. It requests the complete historical data and extract the zipped `taxigov_corridas.csv` file;
    - Stores the extracted CSV file in S3;
    - Creates `raw__taxigov_corridas` Redshift table, if the table already not exists;
    - Insert into `raw__taxigov_corridas` Redshift table, via UPSERT for the unique keys `base_origem` and `qru_corrida`.

<br/>

3. `public_expenses_datawarehouse_dag`:

    - Checks for upstream dependency of `raw__public_expenses_data` table (existance and populated rows) through `UpstreamDependencyCheckOperator`;
    - If the upstream dependency check is sucessful, it creates `dim_expenses`, `dim_cost_centers` and `dim_cost_centers_relationship` dimension tables from `raw__public_expenses_data` table;
    - Checks if all `id` columns created for the dimension tables are unique through `UniqueKeyCheckOperator`; 
    - If the unique key check is sucessful, it creates `fact_monthly_expenses` table with monthly aggregated expense data for the hierarchical cost centers relationship.

<br/>

4. `taxigov_datawarehouse_dag`:

    - Checks for upstream dependency of `raw__taxigov_corridas` table (existance and populated rows) through `UpstreamDependencyCheckOperator`;
    - If the upstream dependency check is sucessful, it creates `dim_rides`, `dim_dates` and `dim_requests` dimension tables from `raw__taxigov_corridas` table;
    - Checks if all `id` columns created for the dimension tables are unique through `UniqueKeyCheckOperator`; 
    - If the unique key check is sucessful, it creates `fact_daily_rides` table with daily aggregated requested taxi rides.

## Tables Schema

### Public Expense Datawarehouse
1. dim_expenses: Table with all government expense events.
  
    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    | id | VARCHAR | Unique key for this table |
    | reference_year | INTEGER | Competence year of expense event  |
    | reference_month | INTEGER | Competence month of expense event |
    | cost_center | VARCHAR | Siconfi Cost Center |
    | expense_category | VARCHAR | Category of the expense event |
    | expense_detail | INTEGER | Nature of the expense event | 
    | expense_value | NUMERIC | Value in BRL of government public expense |

2. dim_cost_centers: Table with all Siconfi Cost Center.
  
    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    | id | VARCHAR | Unique key for this table. It's the Siconfi Cost Center ID |
    | name | VARCHAR | Cost center name |

3. dim_cost_centers_relationship: Table with all Siconfi Cost Center hierarchy. 
  
    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    | id | VARCHAR | Unique key for this table |
    | first_level_cost_center | VARCHAR | ID of the highest level hierarchy of the cost center |
    | second_level_cost_center | VARCHAR | ID of the second level hierarchy of the cost center |
    | third_level_cost_center | VARCHAR | ID of the third level hierarchy of the cost center |
    | fouth_level_cost_center | VARCHAR | ID of the lowest level hierarchy of the cost center. Its a FK for dim_cost_centers table |

4. fact_monthly_expenses: Table with monthly aggregated data for a Siconfi cost center. 
  
    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    | reference_month | INTEGER | Competence month |
    | reference_year | INTEGER | Competence year |
    | first_level_cost_center | VARCHAR | ID of the first level hierarchy of the cost center |
    | second_level_cost_center | VARCHAR | ID of the second level hierarchy of the cost center |
    | third_level_cost_center | VARCHAR | ID of the third level hierarchy of the cost center |
    | fouth_level_cost_center | VARCHAR | ID of the lowest level hierarchy of the cost center. Its a FK for dim_cost_centers table | 
    | total_expense_value | NUMERIC | Total expense of this cost center in this month |

![Expense Table Schema](https://github.com/rmbborges/UNDE-CAPSTONE-PROJECT-taxigov-and-siconfi/blob/main/expense_table_schema.png?raw=true)

### Taxigov Datawarehouse
1. dim_requests: Table with all Taxigov ride requests.
  
    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    | id | VARCHAR | Unique key for this table |
    | requested_by | TIMESTAMP | Government Agency that created this request  |
    | requested_at | TIMESTAMP | Timestamp of the request submission |
    | approved_at | TIMESTAMP | Timestamp of the request approval |
    | reason | VARCHAR | Category of the request nature |
    | requested_dropoff_latitude | NUMERIC | Requested dropoff latitude in Taxigov app | 
    | requested_dropoff_longitude | NUMERIC | Requested dropoff longitude in Taxigov app |
    | commentary | VARCHAR | Request approver commentary |

2. dim_rides: The effective ride (created from a request)
  
    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    | id | VARCHAR | Unique key for this table |
    | request_id | VARCHAR | The request ID (FK dim_requests) |
    | started_at | TIMESTAMP | Timestamp of the start of the ride |
    | ended_at | TIMESTAMP | Timestamp of the end of the ride |
    | pickup_latitude | NUMERIC | Ride pickup latitude |
    | pickup_longitude | NUMERIC | Ride pickup longitude |
    | dropoff_latitude | NUMERIC | Ride dropoff latitude |
    | dropoff_longitude | NUMERIC | Ride dropoff longitude |
    | distance | NUMERIC | The total distance of the ride in kilometers |
    | cost | NUMERIC | Ride cost in BRL |

3. dim_dates: Dimension table for date support. 
  
    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    | id | VARCHAR | Unique key for this table |
    | ts | TIMESTAMP | Timestamp |
    | date | DATE | Date of TS |
    | month | INTEGER | Month of TS |
    | year | INTEGER | Year of TS |
    | day_of_week | INTEGER | The day of week (DOW) of TS |
    | is_weekend | BOOLEAN | If this TS was a weekend |

4. fact_daily_rides: Table with daily aggregated data of taxigov rides.
  
    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    | date | DATE | Date |
    | ride_requests_count | INTEGER | Number of rides requests |
    | rides_count | INTEGER | Number of rides |
    | total_rides_cost | VARCHAR | Total cost in BRL of rides |
    | average_cost_per_kilometer | NUMERIC | Average cost per ride kilometer |
    | average_ride_duration | INTEGER | Average time, in minutes, of this ride | 
    | average_ride_request_sla | INTEGER | Average time, in minutes, of this ride | 
    | average_ride_cost | NUMERIC | Average cost in BRL of rides | 
    | average_ride_distance | INTEGER | Average distance in kilometers of rides | 

![Rides Table Schema](https://github.com/rmbborges/UNDE-CAPSTONE-PROJECT-taxigov-and-siconfi/blob/main/taxigov_table_schema.png?raw=true)