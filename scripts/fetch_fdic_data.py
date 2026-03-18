import os
import json

# Create directory if it doesn't exist
os.makedirs('/opt/airflow/data', exist_ok=True)

# Use hardcoded benchmark data instead of API call
benchmarks = {
    'benchmark_car': 14.0,
    'benchmark_lcr': 120.0,
    'benchmark_npl': 1.5
}

with open('/opt/airflow/data/fdic_benchmark.json', 'w') as f:
    json.dump(benchmarks, f)

print('FDIC benchmark data saved successfully')
