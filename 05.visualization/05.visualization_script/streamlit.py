import streamlit as st
import pandas as pd
import boto3
import io
import plotly.express as px
import warnings

warnings.filterwarnings("ignore", message=".*Boto3 will no longer support Python 3.9.*")

BUCKET = "dohyun-data-mining"
RESULT_PREFIX = "04.analysis/04.analysis_results/"

st.set_page_config(page_title="Multi-Cloud Incident Master Dashboard", layout="wide")

@st.cache_data
def load_csv_from_s3(filename):
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"{RESULT_PREFIX}{filename}")
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
    except Exception as e:
        return None

# 데이터 로드
df_aws_sum = load_csv_from_s3("total_sum.csv")
df_aws_trend = load_csv_from_s3("monthly_trend.csv")
df_aws_cause = load_csv_from_s3("detailed_causes.csv")
df_aws_reg_svc = load_csv_from_s3("region_service_stats.csv")
df_aws_chain = load_csv_from_s3("service_chains.csv")
df_aws_service = load_csv_from_s3("service_impact_all.csv")

df_gcp_sum = load_csv_from_s3("gcp_total_sum.csv")
df_gcp_trend = load_csv_from_s3("gcp_monthly_trend.csv")
df_gcp_cause = load_csv_from_s3("gcp_detailed_causes.csv")
df_gcp_reg_svc = load_csv_from_s3("gcp_region_service_stats.csv")
df_gcp_chain = load_csv_from_s3("gcp_service_chains.csv")
df_gcp_service = load_csv_from_s3("gcp_service_impact_all.csv")

df_aws_lift = load_csv_from_s3("aws_statistical_lift.csv")
df_aws_central = load_csv_from_s3("aws_propagation_centrality.csv")
df_gcp_lift = load_csv_from_s3("gcp_statistical_lift.csv")
df_gcp_central = load_csv_from_s3("gcp_propagation_centrality.csv")

st.title("🛡️ Multi-Cloud Global Incident Master Dashboard")
st.caption("CSP단 장애 통합 분석 및 전파 모델링 가설 검증 대시보드 (AWS & GCP 데이터 마이닝)")

if df_aws_sum is not None and df_gcp_sum is not None:
    st.subheader("📊 클라우드별 인프라 가용성 요약")
    m1, m2, m3, m4 = st.columns(4)
    
    aws_total = df_aws_sum.iloc[0]['Total']
    gcp_total = df_gcp_sum.iloc[0]['Total']
    
    # 💡 건수 비교(delta) 부분 제거 완료
    m1.metric("📦 AWS 총 분석 관측치", f"{aws_total}건", help="PES 및 과거 히스토리 포함 전체 장애 표본 수")
    m2.metric("🟢 GCP 총 분석 관측치", f"{gcp_total}건", help="수집된 글로벌 Major 장애 표본 수")
    m3.metric("🏗️ AWS 모니터링 서비스", f"{len(df_aws_service) if df_aws_service is not None else 0}개")
    m4.metric("🔷 GCP 모니터링 서비스", f"{len(df_gcp_service) if df_gcp_service is not None else 0}개")
    
    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 멀티 클라우드 종합 비교", 
        "🧡 AWS 인프라 상세 분석", 
        "💙 GCP 인프라 상세 분석", 
        "🧠 🔬 통계적 유의성 검정 및 전파 모델링"
    ])

    with tab1:
        st.subheader("📈 CSP 간 가용성 추이 및 동시 발생 패턴 비교")
        if df_aws_trend is not None and df_gcp_trend is not None:
            df_aws_trend['CSP'] = 'AWS'
            df_gcp_trend['CSP'] = 'GCP'
            
            df_total_trend = pd.concat([df_aws_trend, df_gcp_trend], ignore_index=True)
            df_total_trend = df_total_trend.sort_values(by='TS')
            
            fig_trend = px.line(df_total_trend, x='TS', y='Count', color='CSP', markers=True, text='Count',
                                color_discrete_map={'AWS': '#FF9900', 'GCP': '#1A73E8'})
            fig_trend.update_traces(textposition="top center")
            fig_trend.update_layout(
                xaxis_type='category',
                xaxis={'categoryorder': 'category ascending'},
                height=400, 
                yaxis_title="장애 발생 건수", 
                xaxis_title="관측 연월(YYYY-MM)"
            )
            st.plotly_chart(fig_trend, use_container_width=True)

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

        st.divider()

        st.markdown("##### **🏗️ AWS 서비스별 누적 장애 영향도 전수 조사**")
        if df_aws_service is not None:
            chart_height_aws = max(500, len(df_aws_service) * 23)
            fig_aws_svc = px.bar(df_aws_service.sort_values('Count', ascending=True), 
                                x='Count', y='Service', orientation='h', height=chart_height_aws,
                                color='Count', color_continuous_scale='YlOrRd', text_auto=True)
            st.plotly_chart(fig_aws_svc, use_container_width=True)

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

        st.markdown("##### **🏗️ GCP 서비스별 누적 장애 영향도 전수 조사**")
        if df_gcp_service is not None:
            chart_height_gcp = max(500, len(df_gcp_service) * 23)
            fig_gcp_svc = px.bar(df_gcp_service.sort_values('Count', ascending=True), 
                                x='Count', y='Service', orientation='h', height=chart_height_gcp,
                                color='Count', color_continuous_scale='GnBu', text_auto=True)
            st.plotly_chart(fig_gcp_svc, use_container_width=True)

    with tab4:
        st.subheader("🔬 1차 심사 의견 반영: 가설 검증 및 인과성 전파 모델링")
        # 💡 어려운 텍스트를 직관적인 설명으로 변경했습니다.
        st.info("💡 **분석 기준 (시간 윈도우 & 인과성):** 우연히 겹친 장애를 배제하기 위해, ① 동일한 시간대(±2시간 이내)에 발생했거나 ② 장애 보고서 본문 내에 명확한 원인-결과(`caused by` 등)로 기록된 팩트만을 '연쇄 전파 장애'로 인정하여 모델링했습니다.")
        
        st.markdown("### 🧡 1. AWS 인프라 의존성 검정 및 병목점 추적")
        aw_col1, aw_col2 = st.columns(2)
        with aw_col1:
            # 💡 제목 옆에 도움말 아이콘을 달아 쉽게 설명했습니다.
            st.markdown("##### **정량적 의존 규칙 강도 (Statistical Lift Top 10)**", help="두 서비스가 우연히 같이 죽을 확률을 1로 봤을 때, 1보다 크면 클수록 시스템적으로 강하게 엮여서 연쇄 장애를 일으킴을 의미합니다.")
            if df_aws_lift is not None and not df_aws_lift.empty:
                sig_aws_lift = df_aws_lift[df_aws_lift['Significant'] == 'Yes'].head(10)
                if not sig_aws_lift.empty:
                    sig_aws_lift['Pair'] = sig_aws_lift['Service_A'] + " ↔ " + sig_aws_lift['Service_B']
                    fig_aws_lift = px.bar(sig_aws_lift.sort_values('Lift', ascending=True), x='Lift', y='Pair', orientation='h',
                                          color='Lift', color_continuous_scale='Oranges', text_auto='.2f')
                    st.plotly_chart(fig_aws_lift, use_container_width=True)
                    st.caption("💡 해석: Lift 값이 높을수록 두 서비스 간의 연쇄 장애 위험이 큽니다.")
                else:
                    st.info("통계적으로 유의미한 연쇄 장애 페어가 없습니다.")
            else:
                st.warning("AWS Lift 데이터를 로드할 수 없습니다.")
                
        with aw_col2:
            st.markdown("##### **장애 전파 매개 중심성 (Betweenness Centrality Top 10)**", help="수많은 서비스들이 얽힌 네트워크에서, 이 값이 높을수록 장애를 널리 퍼뜨리는 '환승역(SPOF)' 역할을 한다는 뜻입니다.")
            if df_aws_central is not None and not df_aws_central.empty:
                top_aws_central = df_aws_central[df_aws_central['Centrality_Score'] > 0].head(10)
                if not top_aws_central.empty:
                    fig_aws_central = px.bar(top_aws_central.sort_values('Centrality_Score', ascending=True), 
                                             x='Centrality_Score', y='Service', orientation='h',
                                             color='Centrality_Score', color_continuous_scale='Purples', text_auto='.3f')
                    st.plotly_chart(fig_aws_central, use_container_width=True)
                    st.caption("💡 해석: 이 점수가 가장 높은 서비스가 전체 인프라의 가장 치명적인 약점(SPOF)입니다.")
                else:
                    st.info("중심성 지표가 0보다 큰 서비스가 없습니다.")

        st.subheader("", divider="orange")
        
        st.markdown("### 💙 2. GCP 인프라 의존성 검정 및 병목점 추적")
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
                    st.info("💡 검정 결과, GCP는 서비스 간 연쇄 장애(Lift > 1) 패턴이 거의 없는 독립적인 아키텍처임이 확인되었습니다.")
                
        with gcp_col2:
            st.markdown("##### **장애 전파 매개 중심성 (Betweenness Centrality Top 10)**")
            if df_gcp_central is not None and not df_gcp_central.empty:
                top_gcp_central = df_gcp_central.head(10)
                if not top_gcp_central.empty:
                    fig_gcp_central = px.bar(top_gcp_central.sort_values('Centrality_Score', ascending=True), 
                                             x='Centrality_Score', y='Service', orientation='h',
                                             color='Centrality_Score', color_continuous_scale='Teal', text_auto='.3f')
                    st.plotly_chart(fig_gcp_central, use_container_width=True)
else:
    st.error("🚨 S3 데이터 파일 로드 오류! 데이터 분석 결과 파일(.csv)이 S3 버킷에 정상적으로 생성되었는지 확인해 주세요.")