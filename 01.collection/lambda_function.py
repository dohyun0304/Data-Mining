import pandas as pd
import urllib.request
import xml.etree.ElementTree as ET
import json
import boto3
from datetime import datetime, timedelta, timezone
import io

S3_BUCKET = "dohyun-data-mining"
AWS_S3_PATH = "02.origin_data/rss"
GCP_S3_PATH = "02.origin_data/gcp_rss"

def lambda_handler(event, context):
    print("멀티 클라우드(AWS/GCP) Health 데이터 수집 시작")
    
    # 1. KST(한국 표준시) 설정 (UTC+9) - 공통 사용
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    kst_str = now_kst.strftime('%Y%m%d_%H%M%S')
    
    s3 = boto3.client('s3')
    results = {"AWS": "대기", "GCP": "대기"}

    # ==========================================
    # 2. AWS RSS 데이터 가져오기 및 처리
    # ==========================================
    print("AWS RSS Feed 수집 시작")
    aws_url = "https://status.aws.amazon.com/rss/all.rss"
    try:
        with urllib.request.urlopen(aws_url) as response:
            xml_data = response.read()
        
        root = ET.fromstring(xml_data)
        aws_data = []
        
        for item in root.findall('.//item'):
            aws_data.append({
                'Title': item.find('title').text if item.find('title') is not None else '',
                'Link': item.find('link').text if item.find('link') is not None else '',
                'Description': item.find('description').text if item.find('description') is not None else '',
                'Published': item.find('pubDate').text if item.find('pubDate') is not None else '',
                'Collected_At': now_kst.isoformat() # 데이터 내 시간도 KST 반영
            })
        
        if not aws_data:
            print("AWS: 수집할 데이터 없음")
            results["AWS"] = "수집할 데이터 없음"
        else:
            # Pandas 변환 (Layer에서 제공)
            df_aws = pd.DataFrame(aws_data)
            aws_buffer = io.BytesIO()
            df_aws.to_parquet(aws_buffer, index=False, engine='pyarrow')
            
            # S3 업로드 (KST 파일명 사용)
            aws_file_name = f"aws_rss_{kst_str}.parquet"
            s3.put_object(Bucket=S3_BUCKET, Key=f"{AWS_S3_PATH}/{aws_file_name}", Body=aws_buffer.getvalue())
            
            print(f"AWS 업로드 완료: {aws_file_name}")
            results["AWS"] = f"Successfully uploaded {aws_file_name}"
            
    except Exception as e:
        print(f"AWS 에러 발생: {str(e)}")
        results["AWS"] = f"Error: {str(e)}"

    # ==========================================
    # 3. GCP Status JSON 데이터 가져오기 및 처리 (AWS와 동일한 절차)
    # ==========================================
    print("GCP Status JSON 수집 시작")
    gcp_url = "https://status.cloud.google.com/incidents.json"
    try:
        req = urllib.request.Request(gcp_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            json_data = response.read().decode('utf-8')
        
        incidents = json.loads(json_data)
        gcp_data = []
        
        for item in incidents:
            latest_update_text = ""
            if item.get('updates') and len(item['updates']) > 0:
                latest_update_text = item['updates'][0].get('text', '')
            
            impacted_products = item.get('affected_products', item.get('impacted_products', []))
            products_str = ", ".join([p.get('title', '') for p in impacted_products])
            
            gcp_data.append({
                'Title': item.get('external_desc', ''),
                'Link': item.get('uri', ''),
                'Description': latest_update_text,
                'Published': item.get('created', ''),
                'Begin': item.get('begin', ''),
                'End': item.get('end', ''),
                'Impacted_Products': products_str,
                'Collected_At': now_kst.isoformat() # 데이터 내 시간도 KST 반영
            })
        
        if not gcp_data:
            print("GCP: 수집할 데이터 없음")
            results["GCP"] = "수집할 데이터 없음"
        else:
            # Pandas 변환 (Layer에서 제공)
            df_gcp = pd.DataFrame(gcp_data)
            gcp_buffer = io.BytesIO()
            df_gcp.to_parquet(gcp_buffer, index=False, engine='pyarrow')
            
            # S3 업로드 (KST 파일명 사용)
            gcp_file_name = f"gcp_status_{kst_str}.parquet"
            s3.put_object(Bucket=S3_BUCKET, Key=f"{GCP_S3_PATH}/{gcp_file_name}", Body=gcp_buffer.getvalue())
            
            print(f"GCP 업로드 완료: {gcp_file_name}")
            results["GCP"] = f"Successfully uploaded {gcp_file_name}"
            
    except Exception as e:
        print(f"GCP 에러 발생: {str(e)}")
        results["GCP"] = f"Error: {str(e)}"

    # ==========================================
    # 4. 최종 결과 반환
    # ==========================================
    # 두 클라우드 중 하나라도 정상 동작했다면 statusCode를 200으로 반환하여 부분 성공을 허용합니다.
    final_status = 500 if "Error" in results["AWS"] and "Error" in results["GCP"] else 200
    
    return {
        "statusCode": final_status,
        "body": json.dumps(results, ensure_ascii=False)
    }