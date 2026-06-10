import os
import re
from typing import Iterable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def create_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    configure_obs(spark)
    return spark


def configure_obs(spark: SparkSession) -> None:
    ak = os.getenv("OBS_AK")
    sk = os.getenv("OBS_SK")
    endpoint = os.getenv("OBS_ENDPOINT")
    if not (ak and sk and endpoint):
        print("[OBS] OBS_AK/OBS_SK/OBS_ENDPOINT not fully set; skip S3A credential config.")
        return

    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("fs.s3a.access.key", ak)
    hconf.set("fs.s3a.secret.key", sk)
    hconf.set("fs.s3a.endpoint", endpoint)
    hconf.set("fs.s3a.connection.ssl.enabled", "true")
    # 关键修改：使用虚拟主机风格
    hconf.set("fs.s3a.path.style.access", "false")
    # 可选：指定区域
    hconf.set("fs.s3a.endpoint.region", "cn-north-4")
    print(f"[OBS] configured s3a endpoint: {endpoint} (path.style.access=false)")


def read_dataset(spark: SparkSession, path: str) -> DataFrame:
    lower = path.lower()
    if lower.endswith(".parquet") or ".parquet/" in lower:
        return spark.read.parquet(path)
    if lower.endswith(".json") or ".json/" in lower:
        return spark.read.option("multiLine", "false").json(path)
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(path)
    )


def normalize_name(name: str) -> str:
    name = str(name).strip().lower()
    name = re.sub(r"[\s\-./()（）]+", "_", name)
    return name.strip("_")


def find_col(columns: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    normalized = {normalize_name(c): c for c in columns}
    candidate_norms = [normalize_name(c) for c in candidates]

    for cand in candidate_norms:
        if cand in normalized:
            return normalized[cand]

    for norm, original in normalized.items():
        if any(cand in norm or norm in cand for cand in candidate_norms):
            return original
    return None


def standardize_douban(df: DataFrame) -> DataFrame:
    """Map common Douban movie fields to standard names used by analysis.py."""
    cols = df.columns
    id_col = find_col(cols, ["movie_id", "movieid", "id", "电影id", "影片id"])
    title_col = find_col(cols, ["title", "name", "movie_name", "片名", "电影名", "影片名"])
    rating_col = find_col(cols, ["rating", "score", "rate", "douban_score", "评分", "豆瓣评分"])
    genre_col = find_col(cols, ["genre", "genres", "type", "types", "category", "类型", "类别"])
    year_col = find_col(cols, ["year", "release_year", "date", "release_date", "年份", "上映年份", "上映日期"])
    votes_col = find_col(cols, ["votes", "vote", "rating_num", "comment_num", "评价人数", "评分人数", "评论人数"])

    print("[column mapping]", {
        "movie_id": id_col,
        "title": title_col,
        "rating": rating_col,
        "genres": genre_col,
        "year": year_col,
        "votes": votes_col,
    })

    selected = []
    if id_col:
        selected.append(F.col(id_col).cast("string").alias("movie_id"))
    else:
        selected.append(F.monotonically_increasing_id().cast("string").alias("movie_id"))

    if not title_col:
        raise ValueError("Cannot find movie title column. Please update candidate list in common.py.")
    if not rating_col:
        raise ValueError("Cannot find rating column. Please update candidate list in common.py.")

    selected.append(F.col(title_col).cast("string").alias("title"))
    selected.append(F.col(rating_col).cast("double").alias("rating"))

    if genre_col:
        selected.append(F.col(genre_col).cast("string").alias("genres"))
    else:
        selected.append(F.lit(None).cast("string").alias("genres"))

    if year_col:
        selected.append(F.regexp_extract(F.col(year_col).cast("string"), r"(19|20)\d{2}", 0).cast("int").alias("year"))
    else:
        selected.append(F.lit(None).cast("int").alias("year"))

    if votes_col:
        selected.append(F.regexp_replace(F.col(votes_col).cast("string"), r"[^0-9]", "").cast("long").alias("votes"))
    else:
        selected.append(F.lit(None).cast("long").alias("votes"))

    return df.select(*selected)


def missing_ratio(df: DataFrame) -> DataFrame:
    total = df.count()
    if total == 0:
        raise ValueError("Input DataFrame is empty; cannot compute missing ratio.")
    exprs = []
    for c in df.columns:
        exprs.append(
            F.round(
                F.sum(
                    F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), F.lit(1)).otherwise(F.lit(0))
                ) / F.lit(total),
                4,
            ).alias(c)
        )
    return df.select(exprs)


def clean_douban(df_std: DataFrame) -> DataFrame:
    # 策略 1：title/rating 是核心分析字段，缺失会影响排序与聚合，直接删除。
    cleaned = df_std.dropna(subset=["title", "rating"])
    # 策略 2：genres/year 是辅助维度，缺失时用 Unknown/-1 填充，保留样本规模。
    cleaned = cleaned.fillna({"genres": "Unknown", "year": -1})
    return cleaned


def explode_genres(df_clean: DataFrame) -> DataFrame:
    return (
        df_clean.withColumn("genre", F.explode(F.split(F.regexp_replace(F.col("genres"), r"\s+", ""), r"[/|,，、;；]+")))
        .withColumn("genre", F.when(F.col("genre") == "", F.lit("Unknown")).otherwise(F.col("genre")))
    )


def write_result(df: DataFrame, output_path: str, name: str, fmt: str = "csv") -> None:
    if not output_path:
        return
    path = output_path.rstrip("/") + "/" + name
    if fmt == "parquet":
        df.write.mode("overwrite").parquet(path)
    else:
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    print(f"[write] {name} -> {path}")
