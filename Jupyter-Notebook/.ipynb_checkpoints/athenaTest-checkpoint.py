import athenaConnection
import pandas as pd

athena = athenaConnection.AthenaQuery("ap-southeast-1","s3://aws-athena-query-results-358002497134-ap-southeast-1/")

athena_conn, athena_cursor =athena.connect_to_athena()
s3_client=athena.connect_to_s3()
#print("Checking athena works")
#query = "select * from ms_data_stream_production_processed limit 100"
#test_query_results = pd.read_sql(query, athena_conn)
#test_query_results.head(10)

speed_test_query = "select * from ms_data_lake_production.blog_pageview_metrics order by day_str, is_test"
df = athena.query_athena_via_s3_to_dataframe(speed_test_query, athena_cursor, s3_client, print_debug_messages=True)
print("Num records = %i"% len(df))
df.head(10)