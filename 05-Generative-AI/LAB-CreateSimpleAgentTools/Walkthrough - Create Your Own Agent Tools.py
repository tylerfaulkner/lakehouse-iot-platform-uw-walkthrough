# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Walkthrough - Create and Test Your Own Agent Tools
# MAGIC
# MAGIC This notebook will walk you through creating custom Unity Catalog tools for an AI agent in Databricks.
# MAGIC
# MAGIC First, we need to import new packages.

# COMMAND ----------

# MAGIC %pip install unitycatalog-ai[databricks] databricks-langchain -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Update the following cell with the name of your team's catalog (on Databricks Free Edition, keep the default `workspace` catalog).

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "gen_ai_tools"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create an Example Tool
# MAGIC
# MAGIC The following tool is an example of how Agent tools are created and registered within Databricks. 
# MAGIC
# MAGIC The main takeaway from this should be that only a standard python function is needed then registered to Databricks using the `DatabricksFunctionClient`.

# COMMAND ----------

# Read through this — notice how the docstring IS the tool definition.
# The agent uses it to understand when and how to call this function.

from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

def calculate_power_efficiency(actual_output_kw: float, rated_capacity_kw: float) -> str:
    """
    Calculates the power efficiency of a wind turbine as a percentage of its rated capacity.

    Args:
        actual_output_kw (float): The turbine's actual power output in kilowatts.
        rated_capacity_kw (float): The turbine's rated maximum capacity in kilowatts.

    Returns:
        str: A JSON string containing efficiency_pct and a status of 'good', 'degraded', or 'critical'.
    """
    import json
    if rated_capacity_kw <= 0:
        return json.dumps({"error": "rated_capacity_kw must be greater than 0"})
    efficiency = round((actual_output_kw / rated_capacity_kw) * 100, 2)
    status = "good" if efficiency >= 80 else "degraded" if efficiency >= 50 else "critical"
    return json.dumps({"efficiency_pct": efficiency, "status": status})


function_info = client.create_python_function(
    func=calculate_power_efficiency,
    catalog=CATALOG,
    schema=SCHEMA,
    replace=True,
)

print(f"✅ Registered: {function_info.full_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Now, test the function to ensure it works as expected.

# COMMAND ----------

# Quick sanity check
result = client.execute_function(
    function_name=f"{CATALOG}.{SCHEMA}.calculate_power_efficiency",
    parameters={"actual_output_kw": 980.0, "rated_capacity_kw": 2500.0}
)
print(f"Test result: {result.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Your Turn: Register a Unity Catalog Tool
# MAGIC
# MAGIC Now it's your turn to register your own tool using the same pattern from the last section.
# MAGIC
# MAGIC ### Rules
# MAGIC - Every argument **must** have a type hint
# MAGIC - Your docstring **is** the tool's brain — the agent reads it to decide when and how to call your function, so make it descriptive
# MAGIC - Always return a **JSON string**
# MAGIC - Test with `client.execute_function()` before moving to the Playground
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pick One
# MAGIC
# MAGIC **Option A — `calculate_wind_power_estimate(wind_speed_ms: float, rotor_diameter_m: float) -> str`**
# MAGIC Estimate the theoretical power output of a turbine using the wind power formula:
# MAGIC `P = 0.5 × 1.225 × π × (rotor_diameter_m / 2)² × wind_speed_ms³`
# MAGIC Return the estimated output in kW and whether it meets a 1000kW minimum threshold.
# MAGIC
# MAGIC **Option B — `convert_sensor_units(value: float, from_unit: str, to_unit: str) -> str`**
# MAGIC Convert between common turbine sensor units:
# MAGIC `mph ↔ m/s` | `fahrenheit ↔ celsius` | `rpm ↔ rad_per_sec`
# MAGIC Return the converted value and a label describing the conversion applied.
# MAGIC
# MAGIC **Option C — `estimate_maintenance_urgency(days_since_service: int, anomaly_count: int) -> str`**
# MAGIC Score maintenance urgency using simple thresholds:
# MAGIC - `days > 90` **and** `anomalies > 3` → `"urgent"`
# MAGIC - Either condition → `"monitor"`
# MAGIC - Neither → `"ok"`
# MAGIC
# MAGIC Return the urgency level and a recommended action string.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > 💡 **Hint:** Not sure if your tool registered correctly? Check **Unity Catalog → your schema → Functions** in the left nav to confirm it shows up before heading to the Playground.

# COMMAND ----------

# TODO: create your new function below

function_info = client.create_python_function(
    func=calculate_power_efficiency, # TODO: Update with the actual function name
    catalog=CATALOG,
    schema=SCHEMA,
    replace=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, test your new agent tool.

# COMMAND ----------

# Quick sanity check
result = client.execute_function(
    function_name=f"{CATALOG}.{SCHEMA}.YOUR_FUNCTION_NAME", #TODO Replace with your function name
    parameters={} # TODO: add test parameters here
)
print(f"Test result: {result.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Try you new Tools in the Playground
# MAGIC
# MAGIC Congratulations! You have successfully created a new tool. It's time to head to the Plaayground and add them to an AI model to see how they perform!