set HADOOP_HOME=C:\hadoop
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot
set SPARK_LOCAL_IP=127.0.0.1
set PATH=%JAVA_HOME%\bin;C:\hadoop\bin;%PATH%

call venv\Scripts\activate

echo Running ingestion...
python spark_jobs\ingest_balance_sheet.py

echo Running CAR...
python spark_jobs\compute_car.py

echo Running LCR...
python spark_jobs\compute_lcr.py

echo Running NPL...
python spark_jobs\compute_npl.py

echo Running benchmark join...
python spark_jobs\fdic_benchmark_join.py

echo Running risk score...
python spark_jobs\compute_risk_score.py

echo All jobs complete!
pause
