# [WIP]

## Capstone Project Udacity Data Engineering Nanodegree
### Purpose

This project tracks two different sources of government expenses data, both of them made available in Government Transparency Expenses Portal (Portal da Transparência). 

The first data source is Taxigov app. It was developed for brazilian government employees request taxi rides to attend external event. This data is updated daily and consists in a zipped csv file.

The second data source is Brazilian Treasury Department Siconfi API. This source contains all public expenses data, its costs center and all public bids related to. Due to the enormous volume of data available in this API, this project was limited to the Treasury Department itself and a batch of data from 2021 and 2022 was previously processed and loaded to S3. From that, the tesouro_request_dag will request the data daily for the current reference month.
