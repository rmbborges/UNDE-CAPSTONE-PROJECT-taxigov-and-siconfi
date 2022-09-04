# [WIP]

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

1. `public_expenses_request_data.py`:
    
     - Request data from Siconfi API for Treasury Department in the current `month` and `year`;
     - Stores the response JSON data in S3 (the JSON format is ready for Redshift `json 'auto'` copy options);
     - Creates `raw__public_expenses_data` Redshift table, if not exists already;
     - Populate, via UPSERT for the keys, `co_situacao_icc`, `me_referencia`, `an_referencia` and `co_natureza_despesa_deta`) `raw__public_expenses_data` Redshift talbe with the requested data.

2. `taxigov_request_dag.py`: 
    