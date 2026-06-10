import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, "/opt/spark/work")
from common import (  # noqa: E402
    clean_douban,
    create_spark,
    explode_genres,
    missing_ratio,
    read_dataset,
    standardize_douban,
    write_result,
)


def main() -> None:
    spark = create_spark("DoubanSparkAnalysis")
    input_path = os.getenv("INPUT_PATH", "s3a://<BUCKET>/datasets/douban_movies.csv")
    output_path = os.getenv("OUTPUT_PATH", "")
    dataset_type = os.getenv("DATASET_TYPE", "douban").lower()

    if dataset_type != "douban":
        raise ValueError("This template defaults to DATASET_TYPE=douban. For bike data, adapt common.py mappings.")

    print(f"[input] {input_path}")
    raw = read_dataset(spark, input_path)
    raw_count = raw.count()

    print("\n========== A-1. 原始 Schema ==========")
    raw.printSchema()
    print("\n========== A-1. 原始前 5 行 ==========")
    raw.show(5, truncate=False)
    print("\n========== A-1. 原始字段缺失比例 ==========")
    missing_ratio(raw).show(truncate=False, vertical=True)

    std = standardize_douban(raw)
    print("\n========== 标准化后字段 ==========")
    std.show(5, truncate=False)
    print("\n========== 标准化字段缺失比例 ==========")
    missing_ratio(std).show(truncate=False, vertical=True)

    cleaned = clean_douban(std).cache()
    cleaned_count = cleaned.count()

    print("\n========== A-1. 清洗前后行数对比 ==========")
    print(f"raw_count={raw_count}, cleaned_count={cleaned_count}, removed={raw_count - cleaned_count}")

    print("\n========== A-1. 数值字段基本统计 ==========")
    cleaned.select("rating", "year", "votes").describe().show(truncate=False)
    write_result(cleaned, output_path, "cleaned_parquet", fmt="parquet")

    movies = cleaned.createOrReplaceTempView("movies")
    genre_df = explode_genres(cleaned).cache()
    genre_df.createOrReplaceTempView("movie_genres")

    print("\n========== A-2 Q1: GROUP BY 聚合 - 各类型电影数量与平均评分 ==========")
    q1 = spark.sql(
        """
        SELECT
            genre,
            COUNT(*) AS movie_count,
            ROUND(AVG(rating), 3) AS avg_rating,
            ROUND(STDDEV(rating), 3) AS std_rating
        FROM movie_genres
        GROUP BY genre
        HAVING movie_count >= 1
        ORDER BY avg_rating DESC, movie_count DESC
        """
    )
    q1.show(30, truncate=False)
    write_result(q1, output_path, "q1_genre_summary")

    print("\n========== A-2 Q2: ORDER BY Top-N - 评分最高电影 Top 10 ==========")
    q2 = spark.sql(
        """
        SELECT movie_id, title, rating, year, genres, votes
        FROM movies
        WHERE rating IS NOT NULL
        ORDER BY rating DESC, COALESCE(votes, 0) DESC
        LIMIT 10
        """
    )
    q2.show(10, truncate=False)
    write_result(q2, output_path, "q2_top_movies")

    print("\n========== A-2 Q3: 时间维度趋势分析 - 按年份统计 ==========")
    q3 = spark.sql(
        """
        SELECT
            year,
            COUNT(*) AS movie_count,
            ROUND(AVG(rating), 3) AS avg_rating,
            ROUND(PERCENTILE_APPROX(rating, 0.5), 3) AS median_rating
        FROM movies
        WHERE year >= 1900
        GROUP BY year
        ORDER BY year
        """
    )
    q3.show(80, truncate=False)
    write_result(q3, output_path, "q3_year_trend")

    print("\n========== A-2 Q4: JOIN + 窗口函数 - 各类型 Top 3 电影及类型均值 ==========")
    genre_stats = genre_df.groupBy("genre").agg(
        F.count("*").alias("genre_movie_count"),
        F.round(F.avg("rating"), 3).alias("genre_avg_rating"),
    )
    joined = genre_df.join(genre_stats, on="genre", how="left")
    window = Window.partitionBy("genre").orderBy(F.desc("rating"), F.desc(F.coalesce(F.col("votes"), F.lit(0))))
    q4 = (
        joined.withColumn("rank_in_genre", F.row_number().over(window))
        .where(F.col("rank_in_genre") <= 3)
        .select(
            "genre",
            "rank_in_genre",
            "title",
            "rating",
            "year",
            "genre_movie_count",
            "genre_avg_rating",
        )
        .orderBy("genre", "rank_in_genre")
    )
    q4.show(100, truncate=False)
    write_result(q4, output_path, "q4_genre_top3_with_stats")

    cleaned.unpersist()
    genre_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
