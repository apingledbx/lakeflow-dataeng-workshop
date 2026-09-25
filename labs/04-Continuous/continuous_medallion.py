from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# =====================================================================================
# Lab 4 — Continuous medallion pipeline, TWO sources (rate), AutoCDC, MV gold
#
# Running in CONTINUOUS mode, two independent streams flow non-stop:
#   Source 1 (readings)  -> sensor_readings_bronze  (ST, append feed)
#   Source 2 (registry)  -> sensor_registry_bronze  (ST, append feed)
#
# Each bronze feed is collapsed to current-state by its own AUTO CDC (SCD 1) silver ST:
#   sensor_state_silver  = latest temperature per sensor
#   sensor_zone_silver   = current zone per sensor (the registry can move sensors around)
#
# Gold joins the two silvers and aggregates by zone (materialized view):
#   zone_stats_gold      = live per-zone sensor counts + temperature stats
#
# Continuous mode (a pipeline setting, not code) keeps both sources flowing, so the two
# bronze tables grow, both silvers upsert, and the gold MV recomputes on its own.
# =====================================================================================


# ---------- SOURCE 1 -> BRONZE: temperature readings feed ----------------------------
# rate source: one row per tick with a monotonic `value` + `timestamp`. value % 25 fans
# it into 25 sensors that are "read" over and over. reading_seq (the monotonic value) is
# the CDC ordering key: highest per sensor = latest reading.
@dp.table(
    name="sensor_readings_bronze",
    comment="Source 1: raw temperature readings generated continuously by a rate source.",
)
@dp.expect("valid_temperature", "temperature_c BETWEEN -20 AND 60")  # warn-only, like Lab 2
def sensor_readings_bronze():
    return (
        spark.readStream.format("rate").option("rowsPerSecond", "10").load()
        .withColumn("sensor_id", (col("value") % 25).cast("int"))
        .withColumn("temperature_c", expr("round(18 + rand() * 12, 2)"))
        .withColumn("status", expr("CASE WHEN temperature_c > 28 THEN 'HOT' ELSE 'OK' END"))
        .withColumnRenamed("value", "reading_seq")
        .withColumnRenamed("timestamp", "event_ts")
        .select("sensor_id", "temperature_c", "status", "event_ts", "reading_seq")
    )


# ---------- SOURCE 2 -> BRONZE: sensor registry feed ---------------------------------
# A second, independent rate source: the sensor registry, i.e. which zone each sensor is
# in. It's slower (2 rows/s) and the zone rotates slowly (every ~100 events the fleet
# shifts one zone), so AUTO CDC has real changes to apply. registry_seq is its sequence.
@dp.table(
    name="sensor_registry_bronze",
    comment="Source 2: sensor->zone registry changes, generated continuously by a second rate source.",
)
def sensor_registry_bronze():
    return (
        spark.readStream.format("rate").option("rowsPerSecond", "2").load()
        .withColumn("sensor_id", (col("value") % 25).cast("int"))
        .withColumn(
            "zone",
            expr(
                "element_at(array('Zone-North','Zone-South','Zone-East','Zone-West'), "
                "CAST(((value % 25) + floor(value / 100)) % 4 AS INT) + 1)"
            ),
        )
        .withColumnRenamed("value", "registry_seq")
        .withColumnRenamed("timestamp", "updated_ts")
        .select("sensor_id", "zone", "updated_ts", "registry_seq")
    )


# ---------- SILVER 1: current reading per sensor via AUTO CDC (SCD Type 1) ------------
dp.create_streaming_table(
    name="sensor_state_silver",
    comment="Latest reading per sensor, kept current by AUTO CDC (SCD 1) off the readings feed.",
)
dp.create_auto_cdc_flow(
    target="sensor_state_silver",
    source="sensor_readings_bronze",
    keys=["sensor_id"],
    sequence_by="reading_seq",
    stored_as_scd_type=1,
)


# ---------- SILVER 2: current zone per sensor via AUTO CDC (SCD Type 1) ---------------
dp.create_streaming_table(
    name="sensor_zone_silver",
    comment="Current zone per sensor, kept current by AUTO CDC (SCD 1) off the registry feed.",
)
dp.create_auto_cdc_flow(
    target="sensor_zone_silver",
    source="sensor_registry_bronze",
    keys=["sensor_id"],
    sequence_by="registry_seq",
    stored_as_scd_type=1,
)


# ---------- GOLD: join both silvers, aggregate by zone (materialized view) -----------
# Batch reads of the two silvers (no STREAM) joined on sensor_id, grouped by zone, so the
# MV recomputes per-zone stats as either silver changes. Zone is preserved as a dimension.
@dp.materialized_view(
    name="zone_stats_gold",
    comment="Live per-zone sensor counts and temperature stats, joining readings with the registry.",
)
def zone_stats_gold():
    readings = spark.read.table("sensor_state_silver")
    zones = spark.read.table("sensor_zone_silver")
    return (
        readings.join(zones, "sensor_id")
        .groupBy("zone")
        .agg(
            expr("COUNT(*) AS sensor_count"),
            expr("SUM(CASE WHEN status = 'HOT' THEN 1 ELSE 0 END) AS hot_sensors"),
            expr("ROUND(AVG(temperature_c), 2) AS avg_temp_c"),
            expr("ROUND(MIN(temperature_c), 2) AS min_temp_c"),
            expr("ROUND(MAX(temperature_c), 2) AS max_temp_c"),
        )
    )
