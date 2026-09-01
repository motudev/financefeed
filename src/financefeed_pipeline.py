from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

from transformations import aggregate_gold_news, clean_silver_news

VOLUME_PATH = "/Volumes/workspace/dev_roosxfabian_financefeed/raw_financial_news/"

@dp.table(
    name="bronze_news",
    comment="Raw JSON payloads ingested continuously from the Volume."
)
def bronze_news():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schema_checkpoint")
        .load(VOLUME_PATH)
        .withColumn("ingestion_time", current_timestamp())
    )

@dp.table(
    name="silver_news",
    comment="Cleaned news records ready for downstream NLP tasks."
)
@dp.expect_or_drop("valid_headline", "headline IS NOT NULL")
def silver_news():
    df = spark.readStream.table("bronze_news")
    return clean_silver_news(df)

@dp.materialized_view(
    name="gold_news_summary",
    comment="Daily news volume aggregated by source."
)
def gold_news_summary():
    df = spark.read.table("silver_news")
    return aggregate_gold_news(df)