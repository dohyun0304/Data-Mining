import streamlit as st
import pandas as pd
import boto3
import io
import plotly.express as px
import warnings

# Boto3 환경고지 경고 숨기기
warnings.filterwarnings("ignore", message=".*Boto3 will no longer support Python 3.9.*")

BUCKET = "dohyun-data-mining"
RESULT_PREFIX = "04.analysis/04.analysis_results/"

# 1. 스트림릿 페이지 기본 설정
st.set_page_config(page_title="Multi-Cloud Incident Master Dashboard", layout="wide")

@st.cache_data
def load_csv_from_s3(filename):
    """S3 버킷으로부터 분석 결과 CSV 파일을 안전하게 로드하는 캐싱 함수"""
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"{RESULT_PREFIX}{filename}")
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
    except Exception as e:
        # 파일이 아직 생성되지 않았거나 경로가 없을 경우 None 반환
        return None

# ==========================================
# 2. 전수 데이터 로드 (기본 데이터 + 통계/모델링 데이터)
# ==========================================
# [AWS 기본 분석 데이터]
df_aws_sum = load_csv_from_s3("total_sum.csv")
df_aws_trend = load_csv_from_s3("monthly_trend.csv")
df_aws_cause = load_csv_from_s3("detailed_causes.csv")
df_aws_reg_svc = load_csv_from_s3("region_service_stats.csv")
df_aws_chain = load_csv_from_s3("service_chains.csv")
df_aws_service = load_csv_from_s3("service_impact_all.csv")

# [GCP 기본 분석 데이터]
df_gcp_sum = load_csv_from_s3("gcp_total_sum.csv")
df_gcp_trend = load_csv_from_s3("gcp_monthly_trend.csv")
df_gcp_cause = load_csv_from_s3("gcp_detailed_causes.csv")
df_gcp_reg_svc = load_csv_from_s3("gcp_region_service_stats.csv")
df_gcp_chain = load_csv_from_s3("gcp_service_chains.csv")
df_gcp_service = load_csv_from_s3("gcp_service_impact_all.csv")

# [피드백 보완: 통계적 유의성 및 전파 중심성 고급 분석 데이터]
df_aws_lift = load_csv_from_s3("aws_statistical_lift.csv")
df_aws_central = load_csv_from_s3("aws_propagation_centrality.csv")
df_gcp_lift = load_csv_from_s3("gcp_statistical_lift.csv")
df_gcp_central = load_csv_from_s3("gcp_propagation_centrality.csv")


# ==========================================
# 3. 대시보드 헤더 및 상단 메트릭 배치
# ==========================================
st.title("🛡️ Multi-Cloud Global Incident Master Dashboard")
st.caption("CSP단 장애 통합 분석 및 전파 모델링 가설 검증 대시보드 (AWS & GCP 데이터 마이닝)")

# 상단 핵심 메트릭 카드 스코어보드
if df_aws_sum is not None and df_gcp_sum is not None:
    st.subheader("📊 클라우드별 인프라 가용성 요약")
    m1, m2, m3, m4 = st.columns(4)

    aws_total = df_aws_sum.iloc[0]['Total']
    gcp_total = df_gcp_sum.iloc[0]['Total']

    m1.metric("📦 AWS 총 분석 관측치", f"{aws_total}건", help="PES 및 과거 히스토리 포함 전체 장애 표본 수")
    m2.metric("🟢 GCP 총 분석 관측치", f"{gcp_total}건", f"{gcp_total - aws_total}건 비교", delta_color="inverse")
    m3.metric("🏗️ AWS 모니터링 서비스", f"{len(df_aws_service) if df_aws_service is not None else 0}개")
    m4.metric("🔷 GCP 모니터링 서비스", f"{len(df_gcp_service) if df_gcp_service is not None else 0}개")

    st.divider()

    # ==========================================
    # 4. 메인 분석 탭 레이아웃 설계
    # ==========================================
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 멀티 클라우드 종합 비교",
        "🧡 AWS 인프라 상세 분석",
        "💙 GCP 인프라 상세 분석",
        "🧠 🔬 통계적 유의성 검정 및 전파 모델링"
    ])

    # ------------------------------------------
    # TAB 1: 멀티 클라우드 종합 비교
    # ------------------------------------------
    with tab1:
        st.subheader("📈 CSP 간 가용성 추이 및 동시 발생 패턴 비교")
        if df_aws_trend is not None and df_gcp_trend is not None:
            df_aws_trend['CSP'] = 'AWS'
            df_gcp_trend['CSP'] = 'GCP'
            df_total_trend = pd.concat([df_aws_trend, df_gcp_trend], ignore_index=True)

            fig_trend = px.line(df_total_trend, x='TS', y='Count', color='CSP', markers=True, text='Count',
                                color_discrete_map={'AWS': '#FF9900', 'GCP': '#1A73E8'})
            fig_trend.update_traces(textposition="top center")
            fig_trend.update_layout(xaxis_type='category', height=400, yaxis_title="장애 발생 건수", xaxis_title="관측 연월(YYYY-MM)")
            st.plotly_chart(fig_trend, use_container_width=True)

        st.divider()

        st.subheader("🧩 근본 원인(Root Cause) 도메인별 비중 대조")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### **AWS 장애 유발 원인**")
            if df_aws_cause is not None:
                fig_aws_pie = px.pie(df_aws_cause, values='Count', names='Cause', color_discrete_sequence=px.colors.sequential.Oranges_r)
                st.plotly_chart(fig_aws_pie, use_container_width=True)
        with c2:
            st.markdown("#### **GCP 장애 유발 원인**")
            if df_gcp_cause is not None:
                fig_gcp_pie = px.pie(df_gcp_cause, values='Count', names='Cause', color_discrete_sequence=px.colors.sequential.Blues_r)
                st.plotly_chart(fig_gcp_pie, use_container_width=True)

    # ------------------------------------------
    # TAB 2: AWS 인프라 상세 분석
    # ------------------------------------------
    with tab2:
        st.subheader("🧡 AWS 복합 장애 및 리전 분포 분석")
        cc1, cc2 = st.columns(2)
        with cc1:
            st.markdown("##### **장애 원인 대분류 빈도**")
            if df_aws_cause is not None:
                fig_aws_cause_bar = px.bar(df_aws_cause.sort_values('Count'), x='Count', y='Cause',
                                           orientation='h', color='Count', color_continuous_scale='Reds', text_auto=True)
                st.plotly_chart(fig_aws_cause_bar, use_container_width=True)
        with cc2:
            st.markdown("##### **리전 × 서비스 장애 밀집도 (Heatmap)**")
            if df_aws_reg_svc is not None:
                fig_aws_reg = px.density_heatmap(df_aws_reg_svc, x='Region', y='Service', z='Count',
                                                color_continuous_scale='YlOrRd', text_auto=True)
                st.plotly_chart(fig_aws_reg, use_container_width=True)

        st.divider()

        st.markdown("##### **🔗 서비스 간 단순 동시 발생 패턴 (Top 10)**")
        if df_aws_chain is not None and not df_aws_chain.empty:
            df_aws_chain['Combo'] = df_aws_chain['Svc_A'] + " + " + df_aws_chain['Svc_B']
            fig_aws_chain = px.bar(df_aws_chain.head(10), x='Weight', y='Combo', orientation='h',
                                  color='Weight', color_continuous_scale='OrRd', text_auto=True)
            st.plotly_chart(fig_aws_chain, use_container_width=True)
        else:
            st.info("AWS에서 발견된 연쇄 장애 조합이 없습니다.")

        st.divider()

        st.markdown(f"##### **🏗️ AWS 서비스별 누적 장애 영향도 전수 조사**")
        if df_aws_service is not None:
            chart_height_aws = max(500, len(df_aws_service) * 23)
            fig_aws_svc = px.bar(df_aws_service.sort_values('Count', ascending=True),
                                x='Count', y='Service', orientation='h', height=chart_height_aws,
                                color='Count', color_continuous_scale='YlOrRd', text_auto=True)
            st.plotly_chart(fig_aws_svc, use_container_width=True)

    # ------------------------------------------
    # TAB 3: GCP 인프라 상세 분석
    # ------------------------------------------
    with tab3:
        st.subheader("💙 GCP 복합 장애 및 서비스 분포 분석")
        gc1, gc2 = st.columns(2)
        with gc1:
            st.markdown("##### **장애 원인 대분류 빈도**")
            if df_gcp_cause is not None:
                fig_gcp_cause_bar = px.bar(df_gcp_cause.sort_values('Count'), x='Count', y='Cause',
                                           orientation='h', color='Count', color_continuous_scale='Blues', text_auto=True)
                st.plotly_chart(fig_gcp_cause_bar, use_container_width=True)
        with gc2:
            st.markdown("##### **글로벌 서비스 장애 밀집도 (Heatmap)**")
            if df_gcp_reg_svc is not None:
                fig_gcp_reg = px.density_heatmap(df_gcp_reg_svc, x='Region', y='Service', z='Count',
                                                color_continuous_scale='GnBu', text_auto=True)
                st.plotly_chart(fig_gcp_reg, use_container_width=True)

        st.divider()

        st.markdown("##### **🔗 서비스 간 단순 동시 발생 패턴 (Top 10)**")
        if df_gcp_chain is not None and not df_gcp_chain.empty:
            df_gcp_chain['Combo'] = df_gcp_chain['Svc_A'] + " + " + df_gcp_chain['Svc_B']
            fig_gcp_chain = px.bar(df_gcp_chain.head(10), x='Weight', y='Combo', orientation='h',
                                  color='Weight', color_continuous_scale='Blues', text_auto=True)
            st.plotly_chart(fig_gcp_chain, use_container_width=True)
        else:
            st.info("GCP에서 발견된 연쇄 장애 조합이 없습니다.")

        st.divider()

        st.markdown(f"##### **🏗️ GCP 서비스별 누적 장애 영향도 전수 조사**")
        if df_gcp_service is not None:
            chart_height_gcp = max(500, len(df_gcp_service) * 23)
            fig_gcp_svc = px.bar(df_gcp_service.sort_values('Count', ascending=True),
                                x='Count', y='Service', orientation='h', height=chart_height_gcp,
                                color='Count', color_continuous_scale='GnBu', text_auto=True)
            st.plotly_chart(fig_gcp_svc, use_container_width=True)

    # ------------------------------------------
    # TAB 4: 심층 데이터 마이닝 (통계 검정 및 전파 모델링) - 피드백 반영 핵심
    # ------------------------------------------
    with tab4:
        st.subheader("🔬 1차 심사 의견 반영: 가설 검증 및 인과성 전파 모델링")
        st.info("💡 **시간 윈도우 정의:** 최초 인시던트 발생(Begin) 시점 기준 연속적으로 연계되거나, 텍스트 인과 구문 탐색 기법(Rule-based NLP Context)에 의해 구조화된 장애 연쇄 윈도우를 바탕으로 모델링을 수행했습니다.")

        # --- SECTION 1: AWS 통계 및 그래프 모델링 ---
        st.markdown("### 🧡 1. AWS 인프라 의존성 검정 및 병목점 추적")
        st.caption("우연 동시발생 기댓값 분산 대비 통계적 유의성(Fisher's Exact Test p-value < 0.05) 및 네트워크 전파 분석 결과")

        aw_col1, aw_col2 = st.columns(2)
        with aw_col1:
            st.markdown("##### **정량적 의존 규칙 강도 (Statistical Lift Top 10)**")
            if df_aws_lift is not None and not df_aws_lift.empty:
                # 유의미한 관계(Significant == Yes)만 필터링하여 상위 노출
                sig_aws_lift = df_aws_lift[df_aws_lift['Significant'] == 'Yes'].head(10)
                sig_aws_lift['Pair'] = sig_aws_lift['Service_A'] + " ↔ " + sig_aws_lift['Service_B']

                fig_aws_lift = px.bar(sig_aws_lift.sort_values('Lift', ascending=True), x='Lift', y='Pair', orientation='h',
                                      color='Lift', color_continuous_scale='Oranges', text_auto='.2f',
                                      help="Lift > 1: 두 서비스가 독립적인 우연 상태보다 훨씬 높은 상관성으로 연쇄 장애를 일으킴을 의미")
                st.plotly_chart(fig_aws_lift, use_container_width=True)
            else:
                st.warning("AWS 통계적 Lift 데이터 파일(`aws_statistical_lift.csv`)을 로드할 수 없습니다.")

        with aw_col2:
            st.markdown("##### **장애 전파 매개 중심성 (Betweenness Centrality Top 10)**")
            if df_aws_central is not None and not df_aws_central.empty:
                top_aws_central = df_aws_central[df_aws_central['Centrality_Score'] > 0].head(10)

                fig_aws_central = px.bar(top_aws_central.sort_values('Centrality_Score', ascending=True),
                                         x='Centrality_Score', y='Service', orientation='h',
                                         color='Centrality_Score', color_continuous_scale='Purples', text_auto='.3f',
                                         help="매개 중심성이 높을수록 여러 서비스 간의 연쇄 장애 릴레이 경로 상에서 핵심 전파/병목점 역할을 하는 치명적 서비스(SPOF)임을 수학적으로 의미")
                st.plotly_chart(fig_aws_central, use_container_width=True)
            else:
                st.warning("AWS 전파 중심성 데이터 파일(`aws_propagation_centrality.csv`)을 로드할 수 없습니다.")

        st.subheader("", divider="orange")

        # --- SECTION 2: GCP 통계 및 그래프 모델링 ---
        st.markdown("### 💙 2. GCP 인프라 의존성 검정 및 병목점 추적")
        st.caption("글로벌 VPC 구조와 완전 관리형 엔드포인트의 독립 가용성 통계적 증명")

        gcp_col1, gcp_col2 = st.columns(2)
        with gcp_col1:
            st.markdown("##### **정량적 의존 규칙 강도 (Statistical Lift Top 10)**")
            if df_gcp_lift is not None and not df_gcp_lift.empty:
                sig_gcp_lift = df_gcp_lift[df_gcp_lift['Significant'] == 'Yes'].head(10)
                if not sig_gcp_lift.empty:
                    sig_gcp_lift['Pair'] = sig_gcp_lift['Service_A'] + " ↔ " + sig_gcp_lift['Service_B']
                    fig_gcp_lift = px.bar(sig_gcp_lift.sort_values('Lift', ascending=True), x='Lift', y='Pair', orientation='h',
                                          color='Lift', color_continuous_scale='Blues', text_auto='.2f')
                    st.plotly_chart(fig_gcp_lift, use_container_width=True)
                else:
                    st.info("💡 검정 결과, GCP는 서비스 동시 발생 규칙 내 통계적으로 유의미한 수평 전파 결합 패턴이 드문 것으로 분석됩니다. (단일 서비스 중심의 안정성 구조 확인)")
            else:
                st.warning("GCP 통계적 Lift 데이터 파일(`gcp_statistical_lift.csv`)을 로드할 수 없습니다.")

        with gcp_col2:
            st.markdown("##### **장애 전파 매개 중심성 (Betweenness Centrality Top 10)**")
            if df_gcp_central is not None and not df_gcp_central.empty:
                top_gcp_central = df_gcp_central.head(10)
                fig_gcp_central = px.bar(top_gcp_central.sort_values('Centrality_Score', ascending=True),
                                         x='Centrality_Score', y='Service', orientation='h',
                                         color='Centrality_Score', color_continuous_scale='Teal', text_auto='.3f')
                st.plotly_chart(fig_gcp_central, use_container_width=True)
            else:
                st.warning("GCP 전파 중심성 데이터 파일(`gcp_propagation_centrality.csv`)을 로드할 수 없습니다.")

else:
    st.error("🚨 S3 데이터 파일 로드 오류! `advanced_mining.py` 스크립트를 먼저 실행하여 S3 버킷 내의 분석 결과 데이터셋을 최신화해 주세요.")