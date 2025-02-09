from pyflink.table import EnvironmentSettings, TableEnvironment

# ✅ Set up Flink Environment
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# ✅ Create Table
table = t_env.from_elements([("apple", 3), ("banana", 1), ("mango", 2)], ["fruit", "quantity"])

# ✅ Convert Table to List of Rows
rows = list(table.execute().collect())  # Returns Flink Row objects
print("Table Rows:", rows)
