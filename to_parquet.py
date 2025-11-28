import pandas as pd
import argparse
import os

def csv_to_parquet(csv_path, parquet_path=None):
    # Default output path
    if parquet_path is None:
        parquet_path = os.path.splitext(csv_path)[0] + ".parquet"

    # Read CSV
    df = pd.read_csv(csv_path)

    # Combine date + time into a single datetime column (UTC)
    if "date" in df.columns and "time_utc" in df.columns:
        df["datetime_utc"] = pd.to_datetime(df["date"] + " " + df["time_utc"], utc=True)
        df = df.drop(columns=["date", "time_utc"])
        df = df[["datetime_utc"] + [c for c in df.columns if c != "datetime_utc"]]

    # Save to Parquet
    df.to_parquet(parquet_path, index=False)
    print(f"✅ Converted to Parquet: {parquet_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert economic data CSV to Parquet")
    parser.add_argument("--csv", required=True, help="Path to input CSV file")
    parser.add_argument("--out", help="Path to output Parquet file (optional)")
    args = parser.parse_args()

    csv_to_parquet(args.csv, args.out)
