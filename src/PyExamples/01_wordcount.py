from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

# ✅ Set up Batch Processing Mode
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# ✅ Create Data
data = [("apple", 3), ("banana", 1), ("apple", 2)]

# ✅ Create a Table (Lazy - Nothing runs yet)
table = t_env.from_elements(data, ["fruit", "quantity"])
print("✅ Table Created (But not executed yet!)")

# ✅ Apply Transformations (Still Lazy)
result = table.group_by(col("fruit")).select(col("fruit"), col("quantity").sum.alias("total"))
print("✅ Transformations Applied (Still not executed!)")

# ✅ Trigger Execution (Now it runs!)
df = result.to_pandas()
print("✅ Now Execution Happens!")
print(df)
