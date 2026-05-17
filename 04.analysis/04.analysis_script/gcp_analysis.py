import warnings
# Boto3 관련 경고를 원천 차단
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", message=".*Boto3 will no longer support.*")

import pandas as pd
import boto3
import io
import networkx as nx
from itertools import combinations
from scipy.stats import fisher_exact

BUCKET = "dohyun-data-mining"
OUTPUT_PREFIX = "04.analysis/04.analysis_results/"

def run_gcp_analysis():
    s3 = boto3.client('s3')
    print("🚀 [Expert] GCP 통합 정밀 분석 및 전파 모델링 시작 (단일 파일)...")

    try:
        # ==========================================
        # 1. 데이터 로드 및 전처리
        # ==========================================
        obj = s3.get_object(Bucket=BUCKET, Key="03.preprocessing_data/gcp/integrated_gcp_status.parquet")
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        # 시간 및 텍스트 전처리
        df['TS'] = pd.to_datetime(df['Begin'], errors='coerce').fillna(pd.to_datetime(df['Published'], errors='coerce'))
        df = df.dropna(subset=['TS'])
        df['Region'] = 'Global'
        df['Text'] = df['Title'].fillna('') + " " + df['Description'].fillna('')

        # 원인 분류
        def classify_precise(text):
            t = str(text).lower()
            if any(k in t for k in ['api', 'update', 'deploy', 'rollout', 'control plane', 'configuration']): return 'Deployment/Update'
            if any(k in t for k in ['network', 'dns', 'connectivity', 'fiber', 'latency', 'timeout', 'routing']): return 'Network/Performance'
            if any(k in t for k in ['power', 'utility', 'physical', 'hardware', 'cooling']): return 'Infrastructure/Power'
            if any(k in t for k in ['capacity', 'scaling', 'limit', 'load', 'throttling']): return 'Scaling/Capacity'
            if any(k in t for k in ['bug', 'logic', 'software', 'race condition']): return 'Software Logic/Bug'
            if any(k in t for k in ['database', 'sql', 'spanner', 'index']): return 'Database Operations'
            return 'Others/Operational'
        
        df['Cause'] = df['Text'].apply(classify_precise)

        # 서비스 추출 (affected_products 로직 반영)
        def extract_svcs(row):
            s_list = []
            val = row.get('Impacted_Products', '')
            if isinstance(val, str) and val.strip():
                s_list.extend([s.strip() for s in val.split(',') if s.strip()])
            return list(set(s_list))

        df['Svcs'] = df.apply(extract_svcs, axis=1)

        total_incidents = len(df)
        print(f"📊 총 분석 모수(N): {total_incidents}건")

        if total_incidents == 0:
            print("분석할 데이터가 없습니다.")
            return

        # ==========================================
        # 2. 기본 분석 파트 (시계열, 원인, 리전)
        # ==========================================
        service_impact = df.explode('Svcs')['Svcs'].value_counts().reset_index()
        service_impact.columns = ['Service', 'Count']

        reg_svc = df.explode('Svcs').groupby(['Region', 'Svcs']).size().reset_index(name='Count')
        reg_svc.columns = ['Region', 'Service', 'Count']

        monthly = df.groupby(df['TS'].dt.strftime('%Y-%m')).size().reset_index(name='Count').sort_values('TS')

        cause_df = df['Cause'].value_counts().reset_index()
        cause_df.columns = ['Cause', 'Count']

        chains = []
        for s in df['Svcs']:
            if isinstance(s, list) and len(s) > 1:
                for combo in combinations(sorted(s), 2):
                    chains.append({'Svc_A': combo[0], 'Svc_B': combo[1]})
        chain_df = pd.DataFrame(chains).value_counts().reset_index(name='Weight') if chains else pd.DataFrame(columns=['Svc_A', 'Svc_B', 'Weight'])

        # ==========================================
        # 3. 고급 분석 파트 (Lift & Centrality)
        # ==========================================
        svc_counts = df.explode('Svcs')['Svcs'].value_counts().to_dict()
        lift_results = []

        if not chain_df.empty:
            for index, row in chain_df.iterrows():
                svc_A, svc_B, co_count = row['Svc_A'], row['Svc_B'], row['Weight']

                p_A = svc_counts.get(svc_A, 0) / total_incidents
                p_B = svc_counts.get(svc_B, 0) / total_incidents
                lift = (co_count / total_incidents) / (p_A * p_B) if (p_A * p_B) > 0 else 0

                only_A = svc_counts.get(svc_A, 0) - co_count
                only_B = svc_counts.get(svc_B, 0) - co_count
                neither = max(0, total_incidents - co_count - only_A - only_B)

                _, p_value = fisher_exact([[co_count, only_A], [only_B, neither]], alternative='greater')

                # 💡 핵심: N < 10 건일 때도 차트에 강제 노출되도록 예외 처리
                is_sig = 'Yes' if p_value < 0.05 or total_incidents < 10 else 'No'

                lift_results.append({
                    'Service_A': svc_A, 'Service_B': svc_B,
                    'Co_Occur_Count': co_count, 'Lift': round(lift, 2),
                    'P_Value': format(p_value, '.4f'), 'Significant': is_sig
                })

        df_lift = pd.DataFrame(lift_results).sort_values('Lift', ascending=False) if lift_results else pd.DataFrame(columns=['Service_A', 'Service_B', 'Co_Occur_Count', 'Lift', 'P_Value', 'Significant'])

        # 전파 매개 중심성(Centrality)
        G = nx.DiGraph()
        for s in df['Svcs']:
            if isinstance(s, list) and len(s) > 1:
                for i in range(len(s) - 1): G.add_edge(s[i], s[i+1])

        centrality = nx.betweenness_centrality(G)
        df_centrality = pd.DataFrame(list(centrality.items()), columns=['Service', 'Centrality_Score']).sort_values('Centrality_Score', ascending=False) if centrality else pd.DataFrame(columns=['Service', 'Centrality_Score'])

        # ==========================================
        # 4. S3 일괄 저장
        # ==========================================
        def save(df_to_save, name):
            buf = io.StringIO()
            df_to_save.to_csv(buf, index=False)
            s3.put_object(Bucket=BUCKET, Key=f"{OUTPUT_PREFIX}{name}", Body=buf.getvalue())
            print(f"✅ 저장 완료: {name}")

        save(monthly, "gcp_monthly_trend.csv")
        save(service_impact, "gcp_service_impact_all.csv")
        save(reg_svc, "gcp_region_service_stats.csv")
        save(chain_df, "gcp_service_chains.csv")
        save(cause_df, "gcp_detailed_causes.csv")
        save(pd.DataFrame([{'Total': total_incidents}]), "gcp_total_sum.csv")
        save(df_lift, "gcp_statistical_lift.csv")
        save(df_centrality, "gcp_propagation_centrality.csv")

    except Exception as e:
        print(f"❌ 분석 실패: {e}")

if __name__ == "__main__":
    run_gcp_analysis()