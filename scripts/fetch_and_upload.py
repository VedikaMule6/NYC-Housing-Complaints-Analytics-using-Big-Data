import requests
import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime, timedelta


def fetch_filtered_data(base_url, date_field, data_date, limit=1500, agency=None):
    """Fetch data for a specific date using given field (created_date / received_date)."""
    start = f"{data_date}T00:00:00"
    end = f"{data_date}T23:59:59"
    where_clause = f"{date_field} between '{start}' and '{end}'"

    if agency:
        where_clause += f"and agency={agency}"

    url = f"{base_url}?$where={where_clause}&$limit={limit}"

    print(f"🔍 Fetching from: {url}")
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        print(f"Got {len(data)} records for {data_date}")
        return pd.DataFrame(data)
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")


def upload_to_s3(df, bucket, key):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"Uploaded to s3://{bucket}/{key}")


def main():
    # Calculate target dates
    nyc_311_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    hpd_date = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')

    bucket = "cdac-final-project-data"

    # Fetch NYC 311 Complaints (T-1)
    df_311 = fetch_filtered_data(
        base_url="https://data.cityofnewyork.us/resource/erm2-nwe9.json",
        date_field="created_date",
        data_date=nyc_311_date,
        agency="HPD"
    )
    upload_to_s3(df_311, bucket, f"Bronze-level/311_nyc_dataset/{nyc_311_date}.parquet")

    # Fetch HPD Complaints (T-2)
    df_hpd = fetch_filtered_data(
        base_url="https://data.cityofnewyork.us/resource/ygpa-z7cr.json",
        date_field="received_date",
        data_date=hpd_date
    )

    upload_to_s3(df_hpd, bucket, f"raw_data/hpd/{hpd_date}.parquet")


if __name__ == "__main__":
    main()
