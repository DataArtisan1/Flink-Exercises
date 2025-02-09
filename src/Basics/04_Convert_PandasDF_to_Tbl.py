from pyflink.table import EnvironmentSettings, TableEnvironment

# ✅ Set up Flink Environment
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# ✅ Sample Pandas DataFrame
import pandas as pd
pandas_df = pd.DataFrame(
    {"fruit": ["apple", "banana", "mango"],
     "quantity": [3, 1, 2]}
)

# ✅ Convert Pandas DataFrame to Flink Table
schema = ["fruit", "quantity"]
table_from_df = t_env.from_pandas(pandas_df, schema)

# ✅ Print Table from Pandas DataFrame
table_from_df.execute().print()
