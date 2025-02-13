import dlt
import duckdb
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

@dlt.resource(name="taxi_trip_data")   # <--- The name of the resource (will be used as the table name)
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):    # <--- API endpoint for retrieving taxi ride data
        yield page   

# define new dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

# run the pipeline with the new resource
load_info = pipeline.run(ny_taxi)
print(load_info)

# A database '<pipeline_name>.duckdb' was created in working directory so just connect to it

# Connect to the DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# Describe the dataset. Question 2
tables = conn.sql("SHOW TABLES").df()
print(conn.sql("DESCRIBE").df())
print(tables)

# explore loaded data. Question 3
df = pipeline.dataset(dataset_type="default").taxi_trip_data.df()
print(df)

#Calculate the average trip duration in minutes.Question 4
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM taxi_trip_data;
            """
        )
    # Prints column values of the first row
    print(res)
