AWS Global Incident Expert Dashboard
기업 및 개인의 AWS 환경이 아닌 CSP단에서의 장애를 분석합니다.
AWS Health Dashboard의 RSS 피드와 과거 PES(Post-Event Summary) 데이터를 수집하여, 장애 원인을 분석하고 리전 및 서비스별 상관관계를 시각화하는 분석 플랫폼입니다.

1. Project Overview
본 프로젝트는 AWS 인프라의 가용성 데이터를 통합 분석합니다.
RSS, PES, Historical Data 등 다양한 소스에서 수집된 데이터를 바탕으로 장애의 전파력과 서비스 간 연쇄 장애 패턴을 도출하여 클라우드 운영 인사이트를 제공하는 것을 목표로 합니다.

2. Architecture and Workflow
데이터 수집: AWS Lambda와 Amazon EventBridge를 사용하여 10분 주기로 AWS RSS(feedparser) 데이터를 자동 수집합니다.
데이터 전처리: 비용 최적화를 위해 EC2 환경에서 Python Pandas와 re(정규표현식)를 활용합니다. 비정형 텍스트에서 서비스명을 추출하고 데이터 규격(날짜, 리전 등)을 정규화합니다.
데이터 저장: 분석 효율 및 비용 절감을 위해 Pyarrow를 사용하여 고성능 열 지향 저장 포맷인 Parquet 형태로 변환 후 Boto3를 통해 AWS S3에 저장합니다.
데이터 분석: EC2 환경에서 Python Pandas의 그룹화 연산과 itertools를 활용합니다. 서비스 매핑, 장애 원인 분류 및 서비스 간 결합도 분석을 수행합니다.
데이터 시각화: EC2 환경에서 Streamlit 프레임워크와 Plotly 라이브러리를 활용하여 대시보드를 서빙합니다.

3. Tech Stack
Language: Python 3.9+
Data Handling: Pandas, Pyarrow, re, itertools
Cloud Native: AWS Lambda, Amazon EventBridge, AWS S3
Visualization: Streamlit, Plotly
Infrastructure: AWS EC2 (Amazon Linux 2023)

4. Key Features
통합 분석: 전체 분석 건수, 최다 발생 리전, 유효 분석 서비스 수 실시간 집계
월별 장애 발생 추이: 데이터 전수 기간 내 장애 발생 흐름 시각화
장애 원인 세분화: Deployment, Scaling, Network 등 10개 카테고리 정밀 분류를 통한 원인 파악
리전별 서비스 장애 분포: 리전과 서비스 간 상관관계 히트맵 분석
서비스 연쇄 장애 패턴: Multi-Service Impact 상황에서의 서비스 간 연쇄 장애 패턴 도출
서비스 영향도 전수 조사: 전체 AWS 서비스 대상 장애 발생 빈도 및 영향도 통계

5. Implementation Details (Cost-Effective Processing)
AWS Managed Service의 비용 부담을 최소화하기 위해 데이터 처리 파이프라인을 다음과 같이 최적화하였습니다.
Preprocessing & Analysis: Managed ETL 도구 대신 EC2 내 Python Pandas를 활용하여 대량의 데이터를 인메모리 방식으로 빠르게 정제하고 고정 비용 내에서 처리량을 극대화했습니다.
Storage: 행 기반 CSV 대비 압축률과 읽기 성능이 월등한 Parquet 포맷을 채택하여 S3 저장 비용 및 트래픽을 최적화했습니다.
Visualization: 별도의 웹 서버나 WAS 구축 없이 Streamlit을 활용하여 데이터 분석용 단일 엔드포인트 대시보드를 구현했습니다.

6. Requirements Content
requests
feedparser
pandas
pyarrow
boto3
streamlit
plotly