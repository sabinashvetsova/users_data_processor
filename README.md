## About

This is the web service for processing users data in minio.

This repository includes docker-compose.yml which starts containers with minio DB and web service written in Python with FastAPI framework.

To start containers:
`docker-compose up -d`

Once it has started it answers to localhost:8080 with following endpoints:
* `GET /data` - gets all records from DB in JSON format with filters: is_image_exists, min_age and max_age.
* `POST /data` - manually triggers reprocessing the source data in minio.
* `GET /stats` - calculates and returns the average age of users matching the filters: is_image_exists, min_age and max_age.

The service also periodically (once in an hour) processes input data, aggregates it and stores in output location.

## Implementation

* For updating data the service:
  * gets list of objects from minio source prefix
  * divides files into 2 groups: csv and png files
  * reads csv files and extracts user ids
  * matches users ids with images files
  * builds list of rows for output csv file
  * puts output object to minio.


* For retrieving data the service:
  * forms where clause based on filters params
  * uses select_object_content method from minio library to filter rows with the help of SQL syntax.


* For calculating average age the service also:
  * uses aggregate function within minio
  * calculates average_age with the help of python datetime libraries.


* For periodical processing the service:
  * uses FastAPI decorator that modifies a function so it is periodically re-executed every 60 minutes.
