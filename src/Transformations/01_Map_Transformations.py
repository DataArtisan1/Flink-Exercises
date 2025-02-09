from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

# ✅ Set up Flink Batch Processing
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# ✅ Create Data
data = [("apple", 3), ("banana", 1), ("apple", 2)]

# ✅ Create Table
table = t_env.from_elements(data, ["fruit", "quantity"])

# ✅ Apply Map Transformation (Multiply quantity by 2)
mapped_table = table.select(col("fruit"), (col("quantity") * 2).alias("double_quantity"))

# ✅ Trigger Execution
df = mapped_table.to_pandas()
print(df)
