import pandas as pd
import boto3
import io
import warnings

warnings.filterwarnings("ignore", message=".*Boto3 will no longer support Python 3.9.*")
BUCKET = "dohyun-data-mining"
PREFIX = "02.origin_data/rss/"
OUTPUT_KEY = "03.preprocessing_data/rss/integrated_rss.parquet"

def integrate_rss_data():
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    
    if 'Contents' not in response:
        print("통합할 RSS 파일이 없습니다.")
        return

    all_dfs = []
    for obj in response['Contents']:
        if obj['Key'].endswith('.parquet'):
            res = s3.get_object(Bucket=BUCKET, Key=obj['Key'])
            all_dfs.append(pd.read_parquet(io.BytesIO(res['Body'].read())))

    if all_dfs:
        integrated_df = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=['Title', 'Published'])
        buffer = io.BytesIO()
        integrated_df.to_parquet(buffer, index=False)
        s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=buffer.getvalue())
        print(f"RSS 통합 완료 (총 {len(integrated_df)}건)")

if __name__ == "__main__":
    integrate_rss_data()