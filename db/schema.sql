
CREATE TABLE regulatory_metrics (
    bank_id VARCHAR,
    metric_date DATE,
    car FLOAT,
    lcr FLOAT,
    npl FLOAT,
    benchmark_car FLOAT,
    benchmark_lcr FLOAT,
    benchmark_npl FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
