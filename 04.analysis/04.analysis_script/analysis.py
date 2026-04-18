import pandas as pd
import boto3
import io
import warnings
import re
from itertools import combinations

warnings.filterwarnings("ignore", message=".*Boto3 will no longer support Python 3.9.*")
BUCKET = "dohyun-data-mining"
OUTPUT_PREFIX = "04.analysis/04.analysis_results/"

def run_integrated_analysis():
    s3 = boto3.client('s3')
    print("🚀 [Expert] 데이터 정밀 분석 및 컬럼 매칭 시작...")

    # AWS 서비스 화이트리스트 (노이즈 방지)
    AWS_SERVICES = ['S3', 'EC2', 'EBS', 'RDS', 'Lambda', 'DynamoDB', 'IAM', 'Route 53', 'CloudFront', 'VPC', 'Kinesis', 'EKS', 'ECS', 'API Gateway', 'SQS', 'SNS', 'ElastiCache', 'Redshift', 'CloudWatch']

    try:
        # 데이터 로드
        pes_df = pd.read_parquet(io.BytesIO(s3.get_object(Bucket=BUCKET, Key="03.preprocessing_data/pes/aws_pes_history.parquet")['Body'].read()))
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="03.preprocessing_data/service-event-history/")
        history_dfs = [pd.read_parquet(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=obj['Key'])['Body'].read())) 
                       for obj in resp.get('Contents', []) if obj['Key'].endswith('.parquet')]
        hist_df = pd.concat(history_dfs, ignore_index=True)

        # 데이터 통합
        p_sub = pes_df[['Full_Text', 'Event_Date']].rename(columns={'Full_Text': 'Text', 'Event_Date': 'TS'}).assign(Region='Global')
        h_sub = hist_df[['Details', 'Start_Time', 'Region']].rename(columns={'Details': 'Text', 'Start_Time': 'TS'})
        all_data = pd.concat([p_sub, h_sub], ignore_index=True)
        all_data['TS'] = pd.to_datetime(all_data['TS'], errors='coerce')
        all_data = all_data.dropna(subset=['TS'])

        # --- [1] 원인 분류 고도화 (Others 최소화) ---
        def classify_precise(text):
            t = str(text).lower()
            if any(k in t for k in ['api', 'update', 'deploy', 'rollout', 'control plane']): return 'Deployment/Update'
            if any(k in t for k in ['config', 'parameter', 'incorrect', 'manual', 'setting']): return 'Configuration'
            if any(k in t for k in ['network', 'dns', 'connectivity', 'fiber', 'latency', 'timeout']): return 'Network/Performance'
            if any(k in t for k in ['power', 'utility', 'physical', 'hardware', 'cooling', 'generator']): return 'Infrastructure/Power'
            if any(k in t for k in ['capacity', 'scaling', 'limit', 'load', 'throttling', 'concurrency']): return 'Scaling/Capacity'
            if any(k in t for k in ['bug', 'logic', 'software', 'race condition', 'null pointer']): return 'Software Logic/Bug'
            if any(k in t for k in ['database', 'rds', 'dynamo', 'index', 'consistency']): return 'Database Operations'
            if any(k in t for k in ['storage', 'ebs', 's3', 'disk', 'corruption', 'volume']): return 'Storage/Data Integrity'
            if any(k in t for k in ['auth', 'iam', 'token', 'cert', 'security', 'permission']): return 'Security/Access'
            if any(k in t for k in ['maintenance', 'scheduled', 'planned', 'routine']): return 'Maintenance'
            return 'Others/Operational'
        
        all_data['Cause'] = all_data['Text'].apply(classify_precise)

        # --- [2] 서비스 추출 ---
        def extract_svc(text):
            t = str(text).upper()
            return [s for s in AWS_SERVICES if re.search(r'\b' + re.escape(s.upper()) + r'\b', t)]
        all_data['Svcs'] = all_data['Text'].apply(extract_svc)

        # --- [3] 결과 데이터셋 생성 (컬럼명 강제 지정) ---
        # 1. 서비스 영향도 전수 (컬럼명 Service로 확정)
        service_impact = all_data.explode('Svcs')['Svcs'].value_counts().reset_index()
        service_impact.columns = ['Service', 'Count']

        # 2. 리전별 서비스 장애 통계 (컬럼명 Service로 확정)
        reg_svc = all_data.explode('Svcs').groupby(['Region', 'Svcs']).size().reset_index(name='Count')
        reg_svc.columns = ['Region', 'Service', 'Count']

        # 3. 연쇄 장애 (Combo)
        chains = []
        for s in all_data['Svcs']:
            if len(s) > 1:
                for combo in combinations(s, 2): chains.append({'Svc_A': combo[0], 'Svc_B': combo[1]})
        chain_df = pd.DataFrame(chains).value_counts().reset_index(name='Weight') if chains else pd.DataFrame(columns=['Svc_A', 'Svc_B', 'Weight'])

        # 4. 월별 추이
        monthly = all_data.groupby(all_data['TS'].dt.strftime('%Y-%m')).size().reset_index(name='Count').sort_values('TS')

        # 저장
        def save(df, name):
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            s3.put_object(Bucket=BUCKET, Key=f"{OUTPUT_PREFIX}{name}", Body=buf.getvalue())
            print(f"✅ 저장 완료: {name}")

        save(monthly, "monthly_trend.csv")
        save(service_impact, "service_impact_all.csv")
        save(reg_svc, "region_service_stats.csv")
        save(chain_df, "service_chains.csv")
        save(all_data['Cause'].value_counts().reset_index(name='Count').rename(columns={'index':'Cause'}), "detailed_causes.csv")
        save(pd.DataFrame([{'Total': len(all_data)}]), "total_sum.csv")

    except Exception as e:
        print(f"❌ 분석 실패: {e}")

if __name__ == "__main__":
    run_integrated_analysis()