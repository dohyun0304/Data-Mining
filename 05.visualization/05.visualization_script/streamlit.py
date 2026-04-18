import streamlit as st
import pandas as pd
import boto3
import io
import plotly.express as px
import warnings

warnings.filterwarnings("ignore", message=".*Boto3 will no longer support Python 3.9.*")
BUCKET = "dohyun-data-mining"
RESULT_PREFIX = "04.analysis/04.analysis_results/"

st.set_page_config(page_title="AWS Incident Master Dashboard", layout="wide")

@st.cache_data
def load(filename):
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"{RESULT_PREFIX}{filename}")
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
    except: return None

# 데이터 로드
df_sum = load("total_sum.csv")
df_trend = load("monthly_trend.csv")
df_cause = load("detailed_causes.csv")
df_reg_svc = load("region_service_stats.csv")
df_chain = load("service_chains.csv")
df_service = load("service_impact_all.csv")

st.title("🛡️ AWS Global Incident Master Dashboard")

if df_sum is not None:
    # --- 통합 분석 건수, 최다 발생 리전, 분석 서비스 수 ---
    k1, k2, k3 = st.columns(3)
    k1.metric("📦 통합 분석 건수", f"{df_sum.iloc[0]['Total']}건")
    if df_reg_svc is not None:
        top_region = df_reg_svc[df_reg_svc['Region'] != 'Global'].groupby('Region')['Count'].sum().idxmax()
        k2.metric("🌎 최다 발생 리전", top_region)
    k3.metric("🏗️ 분석 서비스 수", f"{len(df_service)}개")

    st.divider()

    # --- [차트] 월별 장애 발생 추이 ---
    st.subheader("📈 월별 장애 발생 추이")
    fig_trend = px.line(df_trend, x='TS', y='Count', markers=True, text='Count',
                        color_discrete_sequence=['#1C83E1'])
    fig_trend.update_traces(textposition="top center")
    fig_trend.update_layout(xaxis_type='category', height=450)
    st.plotly_chart(fig_trend, use_container_width=True)

    st.divider()

    # --- [차트] 장애 원인 세분화 & 리전별 서비스 장애 통계 ---
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🧩 장애 원인 세분화 분석")
        fig_cause = px.bar(df_cause.sort_values('Count'), x='Count', y='Cause', 
                          orientation='h', color='Count', color_continuous_scale='Reds', text_auto=True)
        st.plotly_chart(fig_cause, use_container_width=True)
    
    with c2:
        st.subheader("📍 리전별 서비스 장애 분포")
        fig_reg_svc = px.density_heatmap(df_reg_svc, x='Region', y='Service', z='Count',
                                        color_continuous_scale='YlOrRd', text_auto=True)
        st.plotly_chart(fig_reg_svc, use_container_width=True)

    st.divider()

    # --- [차트] 서비스 간 연쇄 장애 패턴 ---
    st.subheader("🔗 서비스 간 연쇄 장애 패턴")
    if df_chain is not None and not df_chain.empty:
        df_chain['Combo'] = df_chain['Svc_A'] + " + " + df_chain['Svc_B']
        fig_chain = px.bar(df_chain.head(10), x='Weight', y='Combo', orientation='h',
                          color='Weight', color_continuous_scale='OrRd', text_auto=True)
        st.plotly_chart(fig_chain, use_container_width=True)
    else:
        st.info("연쇄 장애 패턴이 발견되지 않았습니다.")

    st.divider()

    # --- [차트] 서비스별 영향도 전수 조사 ---
    st.subheader(f"🏗️ 서비스별 영향도 전수 조사 (총 {len(df_service)}개 서비스)")
    chart_height = max(500, len(df_service) * 25)
    fig_svc = px.bar(df_service.sort_values('Count', ascending=True), 
                    x='Count', y='Service', orientation='h',
                    height=chart_height, color='Count', color_continuous_scale='Viridis', text_auto=True)
    
    fig_svc.update_traces(textangle=-90, textfont_size=12, textposition='inside')
    
    st.plotly_chart(fig_svc, use_container_width=True)

else:
    st.error("데이터 로드 실패. analysis.py를 먼저 실행하여 S3 데이터를 갱신해 주세요.")