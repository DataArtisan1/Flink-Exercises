from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

# ✅ Set up Flink Batch Processing
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# ✅ Create Data
data = [("apple", 3), ("banana", 1), ("mango", 2)]

# ✅ Create Table
table = t_env.from_elements(data, ["fruit", "quantity"])

filtered_table = table.filter(col("quantity") > 1)

df = filtered_table.to_pandas()
print(df)
