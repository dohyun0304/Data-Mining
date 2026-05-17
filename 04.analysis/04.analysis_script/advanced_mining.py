import pandas as pd
import boto3
import io
import networkx as nx
from itertools import combinations
from scipy.stats import fisher_exact

BUCKET = "dohyun-data-mining"
OUTPUT_PREFIX = "04.analysis/04.analysis_results/"

def run_advanced_mining(csp="AWS"):
    s3 = boto3.client('s3')
    print(f"🚀 [{csp}] 통계적 유의성 및 전파 모델링 분석 시작...")

    try:
        # 1. 전처리된 데이터 로드 (AWS 또는 GCP)
        file_key = "03.preprocessing_data/pes/aws_pes_history.parquet" if csp == "AWS" else "03.preprocessing_data/gcp/integrated_gcp_status.parquet"
        obj = s3.get_object(Bucket=BUCKET, Key=file_key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        # 임시 Svcs 컬럼 생성 (기존 로직에서 추출한 서비스 리스트라 가정)
        # (실제 환경에서는 앞서 만든 all_data의 'Svcs' 컬럼을 사용하시면 됩니다)

        total_incidents = len(df)
        print(f"총 분석 모수(N): {total_incidents}건")

        # ==========================================
        # [과제 1] 우연 동시발생(Baseline) 대비 Lift & Fisher 검정
        # ==========================================
        # 1. 단일 서비스 발생 확률 P(A) 계산
        svc_counts = df.explode('Svcs')['Svcs'].value_counts().to_dict()

        # 2. 동시 발생 빈도 P(A ∩ B) 계산
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

            # 향상도(Lift) 계산
            lift = p_A_and_B / (p_A * p_B) if (p_A * p_B) > 0 else 0

            # Fisher's Exact Test를 위한 2x2 교차표 생성
            # [ [A도 B도 발생], [A만 발생] ]
            # [ [B만 발생],     [둘다 미발생] ]
            only_A = svc_counts.get(svc_A, 0) - co_count
            only_B = svc_counts.get(svc_B, 0) - co_count
            neither = total_incidents - co_count - only_A - only_B

            _, p_value = fisher_exact([[co_count, only_A], [only_B, neither]], alternative='greater')

            lift_results.append({
                'Service_A': svc_A,
                'Service_B': svc_B,
                'Co_Occur_Count': co_count,
                'Lift': round(lift, 2),
                'P_Value': format(p_value, '.4f'),
                'Significant': 'Yes' if p_value < 0.05 else 'No'
            })

        df_lift = pd.DataFrame(lift_results).sort_values('Lift', ascending=False)

        # ==========================================
        # [과제 2] 방향성 그래프 기반 전파 모델링 (Centrality)
        # ==========================================
        # 방향성 비순환 그래프(DiGraph) 생성
        G = nx.DiGraph()

        # (단순화를 위해, 리스트 내에서 먼저 언급된 서비스나 로그 시간 기준 선행 서비스를 A -> B로 간주)
        # 실제 적용 시에는 NLP를 통해 식별된 1차/2차 원인 배열을 사용합니다.
        for s in df['Svcs']:
            if isinstance(s, list) and len(s) > 1:
                for i in range(len(s) - 1):
                    G.add_edge(s[i], s[i+1]) # 선행 -> 후행 전파

        # 매개 중심성(Betweenness Centrality) 계산: 이 값이 높을수록 전파의 허브(병목) 역할
        centrality = nx.betweenness_centrality(G)
        df_centrality = pd.DataFrame(list(centrality.items()), columns=['Service', 'Centrality_Score'])
        df_centrality = df_centrality.sort_values('Centrality_Score', ascending=False)

        # ==========================================
        # S3 저장
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