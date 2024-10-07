import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta import *
import logging

from dotenv import load_dotenv
load_dotenv(dotenv_path='.my_env')

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# JAR 파일 다운로드 함수
def download_jar(url, filename, save_path):
    filename = os.path.join(save_path, filename)
    if not os.path.exists(filename):
        logger.info(f"Downloading {filename}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # 다운로드 오류 발생 시 예외 처리
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # chunk가 비어있지 않을 경우에만 기록
                    f.write(chunk)
        logger.info(f"Downloaded {filename}")
    else:
        logger.info(f"{filename} already exists. Skipping download.")

# 필요한 JAR 파일 다운로드
jars_info = {
    "hadoop-aws-3.3.1.jar": "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar",
    "aws-java-sdk-bundle-1.11.901.jar": "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar",
    "hadoop-common-3.3.1.jar": "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.1/hadoop-common-3.3.1.jar",
    "postgresql-42.7.4.jar": "https://jdbc.postgresql.org/download/postgresql-42.7.4.jar"
}

save_path='jars'
for filename, url in jars_info.items():
    download_jar(url, filename, save_path=save_path)
    
# JAR 파일 경로 설정
jars = ','.join([os.path.abspath(os.path.join(save_path, jar)) for jar in jars_info.keys()])

# SparkSession 생성
builder = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .config("spark.jars", jars) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{os.getenv('LOCAL_IP_ADDRESS')}:9002") \
    .config("spark.hadoop.fs.s3a.access.key", "pRWLQmzIoCE5nUKyac1O") \
    .config("spark.hadoop.fs.s3a.secret.key", "8FpYdGdHL14opVBipvvGzjScTMNaQSHOjH9WaUZp") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.driver.extraClassPath", "jars/postgresql-42.7.4.jar")

# Delta Lake 설정 추가
builder = builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
spark = configure_spark_with_delta_pip(builder).getOrCreate()    
    

# Hadoop 설정 직접 수정
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.endpoint", f"http://{os.getenv('LOCAL_IP_ADDRESS')}:9002")
hadoop_conf.set("fs.s3a.access.key", "pRWLQmzIoCE5nUKyac1O")
hadoop_conf.set("fs.s3a.secret.key", "8FpYdGdHL14opVBipvvGzjScTMNaQSHOjH9WaUZp")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")


# Delta Lake에 데이터를 저장하고 분석하는 예시
try:
    # PostgreSQL에서 데이터 가져오기
    l40_metrics_data = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{os.getenv('LOCAL_IP_ADDRESS')}:5432/postgres") \
        .option("dbtable", "l40_metrics") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .load()
    print("postgres_data:", l40_metrics_data.show(truncate=False))

        
    # MinIo 에서 데이터 가져오기
    # JSON 파일 스키마 정의
    schema = StructType([
        StructField("batch_id", IntegerType(), True),
        StructField("error_logs", StringType(), True)
    ])
    
    logger.info("MinIO에서 데이터 읽기 시도 중...")
    minio_data = spark.read \
        .schema(schema) \
        .format("json") \
        .load("s3a://l40-logs-bucket/logs.json")

    minio_data.show(truncate=False)
    
    
    # Delta Lake로 변환하여 저장
    l40_metrics_data.write.format("delta").mode("append").save("s3a://deltalake-bucket/delta/l40_metrics_data")
    minio_data.write.format("delta").mode("append").save("s3a://deltalake-bucket/delta/minio_data")

    # 조인 연산 (id와 customer_id를 기준으로 조인)
    merged_data = l40_metrics_data.join(minio_data, "batch_id")
    merged_data.show(truncate=False)
       
except Exception as e:
    logger.error(f"에러 발생: {str(e)}", exc_info=True)

finally:
    spark.stop()