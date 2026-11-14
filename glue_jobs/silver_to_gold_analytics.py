"""
Glue 5.0 Job: Silver -> Gold YouTube Analytics

Creates three analytics datasets from the existing Silver clean_statistics table:
  1. trending_analytics
  2. channel_analytics
  3. category_analytics

The job is adapted to the actual Silver schema:
  - trending_date is already normalized to yyyy-MM-dd by the Silver ETL.
  - engagement_rate already exists.
  - like_ratio is not a Silver column, so this job derives it as likes / views * 100.
  - clean_reference_data uses snippet_title and region.

Required Glue job parameters:
  --JOB_NAME
  --silver_database       yt_pipeline_silver_dev
  --gold_bucket           yt-data-pipeline-golde-ap-south-1-dev
  --gold_database         yt_pipeline_gold_dev

Output:
  s3://<gold_bucket>/youtube/trending_analytics/
  s3://<gold_bucket>/youtube/channel_analytics/
  s3://<gold_bucket>/youtube/category_analytics/

All outputs are Snappy Parquet, partitioned by region.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# 1. JOB SETUP
# =============================================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "silver_database",
        "gold_bucket",
        "gold_database",
    ],
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

SILVER_DB = args["silver_database"]
GOLD_BUCKET = args["gold_bucket"].replace("s3://", "").strip("/")
GOLD_DB = args["gold_database"]

REGIONS = ["ca", "gb", "in", "us"]

logger.info("Starting Silver-to-Gold YouTube analytics job")
logger.info(f"Silver database: {SILVER_DB}")
logger.info(f"Gold database: {GOLD_DB}")
logger.info(f"Gold bucket: {GOLD_BUCKET}")


# =============================================================================
# 2. READ SILVER STATISTICS
# =============================================================================

logger.info("Reading clean_statistics from the Silver Glue Catalog")

stats_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DB,
    table_name="clean_statistics",
    transformation_ctx="silver_statistics",
)

stats_df = stats_dyf.toDF()

required_stats_columns = [
    "video_id",
    "trending_date",
    "channel_title",
    "category_id",
    "views",
    "likes",
    "dislikes",
    "comment_count",
    "engagement_rate",
    "region",
]

missing_stats = [c for c in required_stats_columns if c not in stats_df.columns]
if missing_stats:
    raise ValueError(
        f"clean_statistics is missing required columns: {missing_stats}. "
        f"Available columns: {stats_df.columns}"
    )

# Keep only the four YouTube regions used by this project.
stats_df = stats_df.filter(F.lower(F.col("region")).isin(REGIONS))

# Silver ETL normalizes trending_date to yyyy-MM-dd.
# The second format is retained as a defensive fallback for historical rows.
stats_df = (
    stats_df
    .withColumn("region", F.lower(F.trim(F.col("region"))))
    .withColumn(
        "trending_date_parsed",
        F.coalesce(
            F.to_date(F.col("trending_date"), "yyyy-MM-dd"),
            F.to_date(F.col("trending_date"), "yy.dd.MM"),
            F.to_date(F.col("trending_date")),
        ),
    )
    .withColumn("views", F.col("views").cast("long"))
    .withColumn("likes", F.col("likes").cast("long"))
    .withColumn("dislikes", F.col("dislikes").cast("long"))
    .withColumn("comment_count", F.col("comment_count").cast("long"))
    .withColumn("category_id", F.col("category_id").cast("long"))
    .withColumn("engagement_rate", F.col("engagement_rate").cast("double"))
    .filter(F.col("video_id").isNotNull())
    .filter(F.col("trending_date_parsed").isNotNull())
)

# The original YouTuber script references like_ratio, which is not present
# in this project's Silver schema. Derive it consistently as likes / views * 100.
stats_df = stats_df.withColumn(
    "like_ratio",
    F.when(
        (F.col("views") > 0) & F.col("likes").isNotNull(),
        F.round(F.col("likes") / F.col("views") * 100, 4),
    ).otherwise(F.lit(None).cast("double")),
)

stats_count = stats_df.count()
logger.info(f"Silver statistics records after filtering: {stats_count}")

if stats_count == 0:
    raise ValueError("No Silver statistics records found for CA, GB, IN, US.")


# =============================================================================
# 3. READ REFERENCE DATA AND BUILD CATEGORY LOOKUP
# =============================================================================

logger.info("Reading clean_reference_data for category names")

try:
    ref_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=SILVER_DB,
        table_name="clean_reference_data",
        transformation_ctx="silver_reference_data",
    )

    ref_df = ref_dyf.toDF()

    required_ref_columns = ["id", "snippet_title", "region"]
    missing_ref = [c for c in required_ref_columns if c not in ref_df.columns]

    if missing_ref:
        raise ValueError(
            f"clean_reference_data is missing columns: {missing_ref}. "
            f"Available columns: {ref_df.columns}"
        )

    # Use the latest record for each category within each region.
    # This prevents historical category snapshots from duplicating statistics.
    if "_ingestion_timestamp" in ref_df.columns:
        ref_window = Window.partitionBy(
            F.lower(F.col("region")),
            F.col("id").cast("long"),
        ).orderBy(F.col("_ingestion_timestamp").desc_nulls_last())
        ref_df = (
            ref_df
            .withColumn("_ref_rank", F.row_number().over(ref_window))
            .filter(F.col("_ref_rank") == 1)
            .drop("_ref_rank")
        )

    category_lookup = (
        ref_df
        .select(
            F.lower(F.trim(F.col("region"))).alias("region"),
            F.col("id").cast("long").alias("category_id"),
            F.col("snippet_title").alias("category_name"),
        )
        .filter(F.col("category_id").isNotNull())
        .dropDuplicates(["region", "category_id"])
    )

    lookup_count = category_lookup.count()
    logger.info(f"Category lookup records: {lookup_count}")

    # Join by both category_id and region. Category IDs can have different
    # labels across region-specific reference datasets.
    stats_df = stats_df.join(
        F.broadcast(category_lookup),
        on=["region", "category_id"],
        how="left",
    )

except Exception as exc:
    logger.warn(
        "Could not load category reference data. "
        f"Continuing with category_name='Unknown'. Details: {str(exc)}"
    )
    stats_df = stats_df.withColumn("category_name", F.lit("Unknown"))

if "category_name" not in stats_df.columns:
    stats_df = stats_df.withColumn("category_name", F.lit("Unknown"))
else:
    stats_df = stats_df.withColumn(
        "category_name",
        F.coalesce(F.col("category_name"), F.lit("Unknown")),
    )


# =============================================================================
# 4. HELPER: WRITE A GOLD DATAFRAME AND UPDATE GLUE CATALOG
# =============================================================================

def write_gold_table(dataframe, table_name, transformation_name):
    """
    Write one Gold DataFrame as Snappy Parquet, partitioned by region,
    and update the corresponding Glue Catalog table.

    Note: The output path is dedicated to this Gold table. The Glue sink
    writes the transformed dataset and updates the Catalog metadata.
    """
    output_path = f"s3://{GOLD_BUCKET}/youtube/{table_name}/"

    logger.info(
        f"Writing {table_name}: {dataframe.count()} rows to {output_path}"
    )

    output_dyf = DynamicFrame.fromDF(
        dataframe,
        glueContext,
        transformation_name,
    )

    sink = glueContext.getSink(
        connection_type="s3",
        path=output_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["region"],
        transformation_ctx=f"sink_{table_name}",
    )

    sink.setCatalogInfo(
        catalogDatabase=GOLD_DB,
        catalogTableName=table_name,
    )
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(output_dyf)

    logger.info(f"Completed Gold table: {table_name}")


# =============================================================================
# 5. GOLD TABLE 1: TRENDING ANALYTICS
#    Daily video performance summaries per region.
# =============================================================================

logger.info("Building Gold table: trending_analytics")

trending_df = (
    stats_df
    .groupBy("region", "trending_date_parsed")
    .agg(
        F.count("video_id").alias("total_videos"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("dislikes").alias("total_dislikes"),
        F.sum("comment_count").alias("total_comments"),
        F.avg("views").alias("avg_views_per_video"),
        F.avg("like_ratio").alias("avg_like_ratio"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.max("views").alias("max_views"),
        F.countDistinct("channel_title").alias("unique_channels"),
        F.countDistinct("category_id").alias("unique_categories"),
    )
    .withColumnRenamed("trending_date_parsed", "trending_date")
    .withColumn("_aggregated_at", F.current_timestamp())
)

write_gold_table(
    trending_df,
    "trending_analytics",
    "trending_analytics",
)


# =============================================================================
# 6. GOLD TABLE 2: CHANNEL ANALYTICS
#    Channel-level performance and ranking within each region.
# =============================================================================

logger.info("Building Gold table: channel_analytics")

channel_df = (
    stats_df
    .groupBy("channel_title", "region")
    .agg(
        F.countDistinct("video_id").alias("total_videos"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("comment_count").alias("total_comments"),
        F.avg("views").alias("avg_views_per_video"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.max("views").alias("peak_views"),
        F.count("trending_date_parsed").alias("times_trending"),
        F.min("trending_date_parsed").alias("first_trending"),
        F.max("trending_date_parsed").alias("last_trending"),
        F.collect_set("category_name").alias("categories"),
    )
)

channel_rank_window = Window.partitionBy("region").orderBy(
    F.col("total_views").desc_nulls_last()
)

channel_df = (
    channel_df
    .withColumn("rank_in_region", F.row_number().over(channel_rank_window))
    .withColumn("_aggregated_at", F.current_timestamp())
)

write_gold_table(
    channel_df,
    "channel_analytics",
    "channel_analytics",
)


# =============================================================================
# 7. GOLD TABLE 3: CATEGORY ANALYTICS
#    Category-level performance and daily share of regional views.
# =============================================================================

logger.info("Building Gold table: category_analytics")

category_df = (
    stats_df
    .groupBy(
        "category_name",
        "category_id",
        "region",
        "trending_date_parsed",
    )
    .agg(
        F.count("video_id").alias("video_count"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("comment_count").alias("total_comments"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.countDistinct("channel_title").alias("unique_channels"),
    )
)

daily_region_window = Window.partitionBy("region", "trending_date_parsed")

category_df = (
    category_df
    .withColumn(
        "view_share_pct",
        F.when(
            F.sum("total_views").over(daily_region_window) > 0,
            F.round(
                F.col("total_views")
                / F.sum("total_views").over(daily_region_window)
                * 100,
                2,
            ),
        ).otherwise(F.lit(0.0)),
    )
    .withColumnRenamed("trending_date_parsed", "trending_date")
    .withColumn("_aggregated_at", F.current_timestamp())
)

write_gold_table(
    category_df,
    "category_analytics",
    "category_analytics",
)


# =============================================================================
# 8. JOB COMPLETION
# =============================================================================

logger.info("All three Gold analytics tables have been written successfully.")
logger.info("Gold outputs:")
logger.info(f"s3://{GOLD_BUCKET}/youtube/trending_analytics/")
logger.info(f"s3://{GOLD_BUCKET}/youtube/channel_analytics/")
logger.info(f"s3://{GOLD_BUCKET}/youtube/category_analytics/")

job.commit()
logger.info("Glue job committed successfully.")
