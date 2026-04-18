import pandas as pd
import urllib.request
import xml.etree.ElementTree as ET
import boto3
from datetime import datetime, timedelta, timezone # timezone, timedelta 추가
import io

S3_BUCKET = "dohyun-data-mining"
S3_PATH = "02.origin_data/rss"

def lambda_handler(event, context):
    print("AWS RSS Feed 수집 시작")
    url = "https://status.aws.amazon.com/rss/all.rss"
    
    # 1. KST(한국 표준시) 설정 (UTC+9)
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    kst_str = now_kst.strftime('%Y%m%d_%H%M%S')
    
    # 2. RSS 데이터 가져오기
    try:
        with urllib.request.urlopen(url) as response:
            xml_data = response.read()
        
        root = ET.fromstring(xml_data)
        data = []
        
        for item in root.findall('.//item'):
            data.append({
                'Title': item.find('title').text if item.find('title') is not None else '',
                'Link': item.find('link').text if item.find('link') is not None else '',
                'Description': item.find('description').text if item.find('description') is not None else '',
                'Published': item.find('pubDate').text if item.find('pubDate') is not None else '',
                'Collected_At': now_kst.isoformat() # 데이터 내 시간도 KST 반영
            })
        
        if not data:
            return {"statusCode": 200, "body": "수집할 데이터 없음"}

        # 3. Pandas 변환 (Layer에서 제공)
        df = pd.DataFrame(data)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        
        # 4. S3 업로드 (KST 파일명 사용)
        file_name = f"aws_rss_{kst_str}.parquet"
        s3 = boto3.client('s3')
        s3.put_object(Bucket=S3_BUCKET, Key=f"{S3_PATH}/{file_name}", Body=parquet_buffer.getvalue())
        
        print(f"업로드 완료: {file_name}")
        return {"statusCode": 200, "body": f"Successfully uploaded {file_name}"}
    
    except Exception as e:
        print(f"에러 발생: {str(e)}")
        return {"statusCode": 500, "body": str(e)}