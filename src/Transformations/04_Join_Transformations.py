from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

# ✅ Set up Flink Batch Processing
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# ✅ Create First Table (Fruits and Quantities)
data = [("apple", 3), ("banana", 1), ("mango", 2), ("apple", 2)]
table = t_env.from_elements(data, ["fruit", "quantity"])

# ✅ Create Second Table (Fruit Prices)
prices = [("apple", 10), ("banana", 5)]
price_table = t_env.from_elements(prices, ["fruit", "price"])

# ✅ Rename columns to avoid ambiguity
table = table.select(col("fruit").alias("t1_fruit"), col("quantity"))
price_table = price_table.select(col("fruit").alias("t2_fruit"), col("price"))

# ✅ Join Tables on "fruit" Column
joined_table = table.join(price_table).where(col("t1_fruit") == col("t2_fruit"))

# ✅ Select columns from both tables
result_table = joined_table.select(col("t1_fruit"), col("quantity"), col("price"))

# ✅ Convert to Pandas DataFrame and Print
df = result_table.to_pandas()
print(df)