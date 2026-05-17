import pandas as pd
import boto3
import io
import warnings
from itertools import combinations

warnings.filterwarnings("ignore", message=".*Boto3 will no longer support Python 3.9.*")
BUCKET = "dohyun-data-mining"
# AWS 결과가 저장되는 경로와 동일하게 맞춤
OUTPUT_PREFIX = "04.analysis/04.analysis_results/"

def run_gcp_integrated_analysis():
    s3 = boto3.client('s3')
    print("🚀 [Expert] GCP 데이터 정밀 분석 및 컬럼 매칭 시작...")

    try:
        # 1. 전처리 완료된 GCP 통합 데이터 로드
        obj = s3.get_object(Bucket=BUCKET, Key="03.preprocessing_data/gcp/integrated_gcp_status.parquet")
        all_data = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        # 2. 분석을 위한 데이터프레임 구조화
        all_data['TS'] = pd.to_datetime(all_data['Begin'], errors='coerce')
        all_data['TS'] = all_data['TS'].fillna(pd.to_datetime(all_data['Published'], errors='coerce'))
        all_data = all_data.dropna(subset=['TS'])

        all_data['Text'] = all_data['Title'].fillna('') + " " + all_data['Description'].fillna('')
        all_data['Region'] = 'Global'

        # --- [1] 원인 분류 고도화 ---
        def classify_precise(text):
            t = str(text).lower()
            if any(k in t for k in ['api', 'update', 'deploy', 'rollout', 'control plane', 'configuration change']): return 'Deployment/Update'
            if any(k in t for k in ['config', 'parameter', 'incorrect', 'manual', 'setting']): return 'Configuration'
            if any(k in t for k in ['network', 'dns', 'connectivity', 'fiber', 'latency', 'timeout', 'routing']): return 'Network/Performance'
            if any(k in t for k in ['power', 'utility', 'physical', 'hardware', 'cooling', 'generator']): return 'Infrastructure/Power'
            if any(k in t for k in ['capacity', 'scaling', 'limit', 'load', 'throttling', 'concurrency', 'exhausted']): return 'Scaling/Capacity'
            if any(k in t for k in ['bug', 'logic', 'software', 'race condition', 'null pointer']): return 'Software Logic/Bug'
            if any(k in t for k in ['database', 'sql', 'spanner', 'index', 'consistency']): return 'Database Operations'
            if any(k in t for k in ['storage', 'disk', 'corruption', 'volume']): return 'Storage/Data Integrity'
            if any(k in t for k in ['auth', 'iam', 'token', 'cert', 'security', 'permission']): return 'Security/Access'
            if any(k in t for k in ['maintenance', 'scheduled', 'planned', 'routine']): return 'Maintenance'
            return 'Others/Operational'

        all_data['Cause'] = all_data['Text'].apply(classify_precise)

        # --- [2] 서비스 추출 ---
        def extract_gcp_svc(products_str):
            if pd.isna(products_str) or not str(products_str).strip():
                return []
            return [s.strip() for s in str(products_str).split(',') if s.strip()]

        all_data['Svcs'] = all_data['Impacted_Products'].apply(extract_gcp_svc)

        # --- [3] 결과 데이터셋 생성 ---
        # 1. 서비스 영향도 전수
        service_impact = all_data.explode('Svcs')['Svcs'].value_counts().reset_index()
        service_impact.columns = ['Service', 'Count']

        # 2. 리전별 서비스 장애 통계
        reg_svc = all_data.explode('Svcs').groupby(['Region', 'Svcs']).size().reset_index(name='Count')
        reg_svc.columns = ['Region', 'Service', 'Count']

        # 3. 연쇄 장애 (Combo)
        chains = []
        for s in all_data['Svcs']:
            if isinstance(s, list) and len(s) > 1:
                for combo in combinations(sorted(s), 2):
                    chains.append({'Svc_A': combo[0], 'Svc_B': combo[1]})

        if chains:
            chain_df = pd.DataFrame(chains).value_counts().reset_index(name='Weight')
        else:
            chain_df = pd.DataFrame(columns=['Svc_A', 'Svc_B', 'Weight'])

        # 4. 월별 추이
        monthly = all_data.groupby(all_data['TS'].dt.strftime('%Y-%m')).size().reset_index(name='Count').sort_values('TS')

        # 5. 원인별 통계
        cause_df = all_data['Cause'].value_counts().reset_index()
        cause_df.columns = ['Cause', 'Count']

        # --- [4] S3 저장 (파일명에 gcp_ 접두어 추가) ---
        def save(df, name):
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            s3.put_object(Bucket=BUCKET, Key=f"{OUTPUT_PREFIX}{name}", Body=buf.getvalue())
            print(f"✅ 저장 완료: {OUTPUT_PREFIX}{name}")

        # 파일명 앞에 'gcp_' 명시
        save(monthly, "gcp_monthly_trend.csv")
        save(service_impact, "gcp_service_impact_all.csv")
        save(reg_svc, "gcp_region_service_stats.csv")
        save(chain_df, "gcp_service_chains.csv")
        save(cause_df, "gcp_detailed_causes.csv")
        save(pd.DataFrame([{'Total': len(all_data)}]), "gcp_total_sum.csv")

    except Exception as e:
        print(f"❌ 분석 실패: {e}")

if __name__ == "__main__":
    run_gcp_integrated_analysis()