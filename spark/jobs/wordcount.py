import os
import sys

sys.path.insert(0, "/opt/spark/work")
from common import create_spark  # noqa: E402


def main() -> None:
    spark = create_spark("WordCount")
    input_path = os.getenv("WORDCOUNT_INPUT_PATH", "s3a://<BUCKET>/sample.txt")
    print(f"[input] {input_path}")

    lines = spark.sparkContext.textFile(input_path)
    word_counts = (
        lines.flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
    )
    print("Top 10 words:", word_counts.take(10))
    spark.stop()


if __name__ == "__main__":
    main()
