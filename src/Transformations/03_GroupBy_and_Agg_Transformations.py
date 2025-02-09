from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

# ✅ Set up Flink Batch Processing
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# ✅ Create Data
data = [("apple", 3), ("banana", 1), ("mango", 2), ("apple",2)]

table = t_env.from_elements(data, ["fruit", "quantity"])

grouped_table = (table.group_by(col("fruit"))
                 .select(col("fruit"),
                                col("quantity").sum.alias("total_quantity")))
df = grouped_table.to_pandas()
print(df)
