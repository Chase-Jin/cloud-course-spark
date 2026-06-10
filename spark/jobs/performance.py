import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, "/opt/spark/work")


def run_spark(input_path: str, output_json: str) -> dict:
    from pyspark.sql import functions as F
    from common import clean_douban, create_spark, explode_genres, read_dataset, standardize_douban

    spark = create_spark("SparkPerformanceQ1")
    t0 = time.perf_counter()
    raw = read_dataset(spark, input_path)
    cleaned = clean_douban(standardize_douban(raw))
    genre_df = explode_genres(cleaned)
    result = genre_df.groupBy("genre").agg(F.count("*").alias("movie_count"), F.avg("rating").alias("avg_rating"))
    # 触发实际执行。orderBy 会引入 shuffle，更接近 A-2 Q1。
    rows = result.orderBy(F.desc("avg_rating")).collect()
    elapsed = time.perf_counter() - t0
    payload = {
        "engine": "spark",
        "executors": int(os.getenv("EXECUTOR_INSTANCES", "1")),
        "time_sec": round(elapsed, 4),
        "result_rows": len(rows),
    }
    print("[performance]", json.dumps(payload, ensure_ascii=False))
    if output_json:
        Path(output_json).parent.mkdir(parents=True, exist_ok=True)
        Path(output_json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    spark.stop()
    return payload


def normalize_name(name: str) -> str:
    import re
    return re.sub(r"[\s\-./()（）]+", "_", str(name).strip().lower()).strip("_")


def find_col(columns, candidates):
    norm = {normalize_name(c): c for c in columns}
    cand_norm = [normalize_name(c) for c in candidates]
    for c in cand_norm:
        if c in norm:
            return norm[c]
    for n, original in norm.items():
        if any(c in n or n in c for c in cand_norm):
            return original
    return None


def run_pandas(input_path: str, output_json: str) -> dict:
    if input_path.startswith("s3a://"):
        raise ValueError("Pandas does not read s3a:// directly in this template. Download OBS data to local first.")
    import pandas as pd

    t0 = time.perf_counter()
    df = pd.read_csv(input_path)
    title_col = find_col(df.columns, ["title", "name", "movie_name", "片名", "电影名", "影片名"])
    rating_col = find_col(df.columns, ["rating", "score", "rate", "douban_score", "评分", "豆瓣评分"])
    genre_col = find_col(df.columns, ["genre", "genres", "type", "types", "category", "类型", "类别"])
    if not (title_col and rating_col):
        raise ValueError("Cannot find title/rating columns. Please update performance.py candidates.")
    work = pd.DataFrame({
        "title": df[title_col].astype(str),
        "rating": pd.to_numeric(df[rating_col], errors="coerce"),
        "genres": df[genre_col].fillna("Unknown").astype(str) if genre_col else "Unknown",
    })
    work = work.dropna(subset=["title", "rating"])
    work["genre"] = work["genres"].str.replace(r"\s+", "", regex=True).str.split(r"[/|,，、;；]+")
    genre_df = work.explode("genre")
    result = genre_df.groupby("genre", dropna=False).agg(movie_count=("title", "count"), avg_rating=("rating", "mean"))
    result = result.sort_values(["avg_rating", "movie_count"], ascending=[False, False])
    elapsed = time.perf_counter() - t0
    payload = {
        "engine": "pandas",
        "executors": 0,
        "time_sec": round(elapsed, 4),
        "result_rows": int(len(result)),
    }
    print("[performance]", json.dumps(payload, ensure_ascii=False))
    if output_json:
        Path(output_json).parent.mkdir(parents=True, exist_ok=True)
        Path(output_json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="A-3 performance comparison for Q1 genre aggregation.")
    parser.add_argument("--engine", choices=["pandas", "spark"], required=True)
    parser.add_argument("--input-path", default=os.getenv("INPUT_PATH", ""))
    parser.add_argument("--dataset-type", default=os.getenv("DATASET_TYPE", "douban"))
    parser.add_argument("--output-json", default=os.getenv("PERF_OUTPUT_JSON", ""))
    args = parser.parse_args()
    if args.dataset_type.lower() != "douban":
        raise ValueError("This performance template currently targets douban data.")
    if not args.input_path:
        raise ValueError("--input-path or INPUT_PATH is required")

    if args.engine == "pandas":
        run_pandas(args.input_path, args.output_json)
    else:
        run_spark(args.input_path, args.output_json)


if __name__ == "__main__":
    main()
