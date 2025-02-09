from pyflink.table import EnvironmentSettings, TableEnvironment

# Batch processing (like PySpark's DataFrame API)
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# Drop the temporary view if it exists
# t_env.drop_temporary_view("source")

data = [("hello", 1), ("world", 1), ("hello", 1)]

# Create source table from data
source_table = t_env.from_elements(data, ["word", "word_count"])

# Register the table as a temporary view
t_env.create_temporary_view("source", source_table)

# Execute SQL query
result = t_env.sql_query(""" SELECT word, SUM(word_count) AS total FROM source GROUP BY word """)

# Collect and print the result
result.execute().print()
