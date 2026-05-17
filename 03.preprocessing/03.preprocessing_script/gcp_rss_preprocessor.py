import pandas as pd
import boto3
import io
import warnings

warnings.filterwarnings("ignore", message=".*Boto3 will no longer support Python 3.9.*")
BUCKET = "dohyun-data-mining"
# 람다 수집 코드에서 지정했던 GCP 데이터 경로
PREFIX = "02.origin_data/gcp_rss/"
# 전처리 완료된 GCP 데이터를 저장할 경로
OUTPUT_KEY = "03.preprocessing_data/gcp/integrated_gcp_status.parquet"

def integrate_gcp_data():
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)

    if 'Contents' not in response:
        print("통합할 GCP 파일이 없습니다.")
        return

    all_dfs = []
    for obj in response['Contents']:
        if obj['Key'].endswith('.parquet'):
            res = s3.get_object(Bucket=BUCKET, Key=obj['Key'])
            all_dfs.append(pd.read_parquet(io.BytesIO(res['Body'].read())))

    if all_dfs:
        # GCP JSON 데이터에도 'Title'과 'Published' 컬럼이 존재하므로 동일하게 중복 제거 적용
        integrated_df = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=['Title', 'Published'])
        buffer = io.BytesIO()
        integrated_df.to_parquet(buffer, index=False)
        s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=buffer.getvalue())
        print(f"GCP 통합 완료 (총 {len(integrated_df)}건)")

if __name__ == "__main__":
    integrate_gcp_data()