from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.common import Row

# ✅ Set up Flink Environment
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# ✅ Sample Row Data
rows = [Row("apple", 3), Row("banana", 1), Row("mango", 2)]

# ✅ Convert Rows to Tuples
tuple_rows = [tuple(row) for row in rows]

# ✅ Convert List of Tuples to Flink Table
schema = ["fruit", "quantity"]
table_from_rows = t_env.from_elements(tuple_rows, schema)

# ✅ Print Table from Rows
table_from_rows.execute().print()
