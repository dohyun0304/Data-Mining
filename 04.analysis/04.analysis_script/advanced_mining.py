import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", message=".*Boto3 will no longer support.*")

import pandas as pd
import boto3
import io
import re
import networkx as nx
from itertools import combinations
from scipy.stats import fisher_exact

BUCKET = "dohyun-data-mining"
OUTPUT_PREFIX = "04.analysis/04.analysis_results/"

AWS_SERVICES = ['S3', 'EC2', 'EBS', 'RDS', 'Lambda', 'DynamoDB', 'IAM', 'Route 53', 'CloudFront', 'VPC', 'Kinesis', 'EKS', 'ECS', 'API Gateway', 'SQS', 'SNS', 'ElastiCache', 'Redshift', 'CloudWatch']

def run_advanced_mining(csp="AWS"):
    s3 = boto3.client('s3')
    print(f"🚀 [{csp}] 통계적 유의성 및 전파 모델링 분석 시작...")

    try:
        # ==========================================
        # 1. CSP별 데이터 로드 및 'Svcs' 컬럼 동적 생성
        # ==========================================
        if csp == "AWS":
            pes_obj = s3.get_object(Bucket=BUCKET, Key="03.preprocessing_data/pes/aws_pes_history.parquet")
            pes_df = pd.read_parquet(io.BytesIO(pes_obj['Body'].read()))

            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="03.preprocessing_data/service-event-history/")
            history_dfs = []
            for obj in resp.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    h_obj = s3.get_object(Bucket=BUCKET, Key=obj['Key'])
                    history_dfs.append(pd.read_parquet(io.BytesIO(h_obj['Body'].read())))

            hist_df = pd.concat(history_dfs, ignore_index=True) if history_dfs else pd.DataFrame()

            p_sub = pes_df[['Full_Text']].rename(columns={'Full_Text': 'Text'})
            h_sub = hist_df[['Details']].rename(columns={'Details': 'Text'}) if not hist_df.empty else pd.DataFrame(columns=['Text'])
            df = pd.concat([p_sub, h_sub], ignore_index=True)

            def extract_aws_svc(text):
                t = str(text).upper()
                return [s for s in AWS_SERVICES if re.search(r'\b' + re.escape(s.upper()) + r'\b', t)]

            df['Svcs'] = df['Text'].apply(extract_aws_svc)

        else: # GCP
            gcp_obj = s3.get_object(Bucket=BUCKET, Key="03.preprocessing_data/gcp/integrated_gcp_status.parquet")
            df = pd.read_parquet(io.BytesIO(gcp_obj['Body'].read()))

            # GCP 서비스 추출 함수 완벽 수정 (JSON 스키마 반영)
            def extract_gcp_svc(row):
                services = []

                # 1. 수집 파이프라인에서 문자열로 합쳐서 저장한 경우 ('Impacted_Products' 또는 'Affected_Products')
                target_col = None
                if 'affected_products' in row.index: target_col = row['affected_products']
                elif 'Affected_Products' in row.index: target_col = row['Affected_Products']
                elif 'Impacted_Products' in row.index: target_col = row['Impacted_Products']

                if isinstance(target_col, str) and target_col.strip():
                    services.extend([s.strip() for s in target_col.split(',') if s.strip()])

                # 2. Raw JSON을 그대로 Parquet로 만든 경우 (리스트 형태의 딕셔너리)
                # 예: [{"title":"Agent Assist", "id":"..."}, ...]
                elif isinstance(target_col, list):
                    for item in target_col:
                        if isinstance(item, dict) and 'title' in item:
                            services.append(item['title'].strip())

                return list(set(services)) # 중복 제거 후 반환

            # DataFrame의 apply에 axis=1을 주어 row 단위로 탐색하도록 변경
            df['Svcs'] = df.apply(extract_gcp_svc, axis=1)

        total_incidents = len(df)
        print(f"총 분석 모수(N): {total_incidents}건")

        if total_incidents == 0:
            print("분석할 데이터가 없습니다.")
            return

        # ==========================================
        # 2. 우연 동시발생(Baseline) 대비 Lift & Fisher 검정
        # ==========================================
        svc_counts = df.explode('Svcs')['Svcs'].value_counts().to_dict()

        co_occur = []
        for s in df['Svcs']:
            if isinstance(s, list) and len(s) > 1:
                for combo in combinations(sorted(s), 2):
                    co_occur.append(combo)

        combo_counts = pd.Series(co_occur).value_counts().reset_index()
        combo_counts.columns = ['Combo', 'Co_Count']

        lift_results = []
        for index, row in combo_counts.iterrows():
            svc_A, svc_B = row['Combo']
            co_count = row['Co_Count']

            p_A = svc_counts.get(svc_A, 0) / total_incidents
            p_B = svc_counts.get(svc_B, 0) / total_incidents
            p_A_and_B = co_count / total_incidents

            lift = p_A_and_B / (p_A * p_B) if (p_A * p_B) > 0 else 0

            only_A = svc_counts.get(svc_A, 0) - co_count
            only_B = svc_counts.get(svc_B, 0) - co_count
            neither = total_incidents - co_count - only_A - only_B
            neither = max(0, neither) # 음수 방지

            _, p_value = fisher_exact([[co_count, only_A], [only_B, neither]], alternative='greater')

            lift_results.append({
                'Service_A': svc_A,
                'Service_B': svc_B,
                'Co_Occur_Count': co_count,
                'Lift': round(lift, 2),
                'P_Value': format(p_value, '.4f'),
                'Significant': 'Yes' if p_value < 0.05 else 'No'
            })

        df_lift = pd.DataFrame(lift_results).sort_values('Lift', ascending=False) if lift_results else pd.DataFrame(columns=['Service_A', 'Service_B', 'Co_Occur_Count', 'Lift', 'P_Value', 'Significant'])

        # ==========================================
        # 3. 방향성 그래프 기반 전파 모델링 (Centrality)
        # ==========================================
        G = nx.DiGraph()

        for s in df['Svcs']:
            if isinstance(s, list) and len(s) > 1:
                for i in range(len(s) - 1):
                    G.add_edge(s[i], s[i+1])

        centrality = nx.betweenness_centrality(G)
        if centrality:
            df_centrality = pd.DataFrame(list(centrality.items()), columns=['Service', 'Centrality_Score'])
            df_centrality = df_centrality.sort_values('Centrality_Score', ascending=False)
        else:
            df_centrality = pd.DataFrame(columns=['Service', 'Centrality_Score'])

        # ==========================================
        # 4. 결과 저장
        # ==========================================
        def save(df_to_save, name):
            buf = io.StringIO()
            df_to_save.to_csv(buf, index=False)
            s3.put_object(Bucket=BUCKET, Key=f"{OUTPUT_PREFIX}{name}", Body=buf.getvalue())
            print(f"✅ 저장 완료: {name}")

        prefix = "gcp_" if csp == "GCP" else "aws_"
        save(df_lift, f"{prefix}statistical_lift.csv")
        save(df_centrality, f"{prefix}propagation_centrality.csv")

    except Exception as e:
        print(f"❌ 분석 실패: {e}")

if __name__ == "__main__":
    run_advanced_mining(csp="AWS")
    run_advanced_mining(csp="GCP")