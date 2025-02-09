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
