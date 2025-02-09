from pyflink.common import Row
from pyflink.table import EnvironmentSettings, TableEnvironment

# ✅ Set up Flink Batch Processing
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# ✅ Create Table from Sample Data
data = [("apple", 3), ("banana", 1), ("mango", 2)]
table = t_env.from_elements(data, ["fruit", "quantity"])

# ✅ Get Schema Information
schema = table.get_schema().get_field_names()
print("Table Schema:", schema)

# ✅ Print Table Contents
print("Table Contents:")
table.execute().print()

# ✅ Convert Table to Pandas DataFrame
pandas_df = table.to_pandas()
print("Pandas DataFrame:")
print(pandas_df)

# ✅ Convert Pandas DataFrame to Table
print("Table from Pandas DataFrame:")
table_from_df = t_env.from_pandas(pandas_df, schema)
table_from_df.execute().print()

# ✅ Convert Table to List of Rows (Fixed)
rows = list(table.execute().collect())  # Returns Flink Row objects
print("Table Rows:", rows)

# ✅ Convert Rows to Tuples before Creating New Table
tuple_rows = [tuple(row) for row in rows]  # ✅ Convert Rows to Tuples

# ✅ Convert List of Tuples to Pandas DataFrame
df_from_rows = t_env.from_elements(tuple_rows, schema).to_pandas()
print("DataFrame from Rows:")
print(df_from_rows)

# ✅ Convert List of Tuples to Table (Fixed)
table_from_rows = t_env.from_elements(tuple_rows, schema)
table_from_rows.execute().print()

# ✅ Print the count of rows (Fixed)
row_count = len(tuple_rows)  # ✅ No need for another execute()
print("Row Count:", row_count)
