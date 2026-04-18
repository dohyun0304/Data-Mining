import pandas as pd
import boto3
import io
import re
import warnings
from datetime import datetime

warnings.filterwarnings("ignore", message=".*Boto3 will no longer support Python 3.9.*")

BUCKET = "dohyun-data-mining"
INPUT_PREFIX = "02.origin_data/service-event-history/"
OUTPUT_PREFIX = "03.preprocessing_data/service-event-history/"

def run_preprocessing():
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=INPUT_PREFIX)
    
    if 'Contents' not in response:
        print("처리할 파일이 없습니다.")
        return

    # 정규표현식 패턴을 미리 정의
    split_pattern = r' - |\t|\s{2,}'
    region_pattern = r'\((.*?)\)'

    for obj in response['Contents']:
        key = obj['Key']
        if not key.endswith('.txt'): continue
        
        file_name = key.split('/')[-1].replace('.txt', '')
        print(f"\n🔍 {file_name} 파싱 시작...")

        res = s3.get_object(Bucket=BUCKET, Key=key)
        content = res['Body'].read().decode('utf-8')
        lines = content.strip().split('\n')

        if not lines: continue

        data = []
        for line in lines:
            line = line.strip()
            if not line: continue

            # 구분자로 쪼개기
            parts = re.split(split_pattern, line)
            
            if len(parts) >= 2:
                try:
                    # 1. 서비스 및 리전 추출
                    header = parts[0]
                    region_match = re.search(region_pattern, header)
                    region = region_match.group(1) if region_match else "Global"
                    service = header.split(' (')[0] if ' (' in header else header
                    
                    # 2. 날짜 추출
                    date_str = parts[1]
                    event_date = pd.to_datetime(date_str)
                    
                    # 3. 상세 내용
                    details = parts[2] if len(parts) > 2 else header

                    data.append({
                        'Service': service,
                        'Region': region,
                        'Details': details,
                        'Start_Time': event_date,
                        'End_Time': event_date,
                        'Source_File': f"{file_name}.txt"
                    })
                except:
                    continue

        if data:
            df = pd.DataFrame(data)
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            s3.put_object(Bucket=BUCKET, Key=f"{OUTPUT_PREFIX}{file_name}.parquet", Body=buffer.getvalue())
            count = len(data)
            print(f"완료: {file_name} ({count}건 저장됨)")
        else:
            first_line = lines[0].strip()
            debug_split = re.split(split_pattern, first_line)
            split_count = len(debug_split)
            print(f"파싱 실패: 형식이 맞지 않습니다.")
            print(f"   ㄴ 첫 줄 샘플: {first_line}")
            print(f"   ㄴ 분석 조각 개수: {split_count}")

if __name__ == "__main__":
    run_preprocessing()