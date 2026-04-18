import pandas as pd
import boto3
import io
import re
import warnings

warnings.filterwarnings("ignore", message=".*Boto3 will no longer support Python 3.9.*")
BUCKET = "dohyun-data-mining"
PREFIX = "02.origin_data/pes/"
OUTPUT_KEY = "03.preprocessing_data/pes/aws_pes_history.parquet"

def preprocess_pes():
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    
    if 'Contents' not in response:
        print("PES 원본 파일이 없습니다.")
        return

    all_data = []
    for obj in response['Contents']:
        key = obj['Key']
        if not key.endswith('.txt'): continue
        
        file_name = key.split('/')[-1]
        date_match = re.search(r'(\d{4})(\d{2})', file_name)
        event_date = f"{date_match.group(1)}-{date_match.group(2)}-01" if date_match else "Unknown"
        
        res = s3.get_object(Bucket=BUCKET, Key=key)
        all_data.append({
            'Title': file_name.replace('.txt', ''),
            'Event_Date': event_date,
            'Full_Text': res['Body'].read().decode('utf-8')
        })

    if all_data:
        df = pd.DataFrame(all_data)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=buffer.getvalue())
        print("PES 전처리 완료")

if __name__ == "__main__":
    preprocess_pes()