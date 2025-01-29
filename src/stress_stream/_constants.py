import os

S2_AUTH_TOKEN = os.getenv("S2_AUTH_TOKEN")

METRICS_NAMESPACE = "stress"

HISTOGRAM_BUCKETS = (
    0.010,
    0.020,
    0.030,
    0.040,
    0.050,
    0.075,
    0.100,
    0.200,
    0.300,
    0.400,
    0.500,
    0.750,
    1.0,
    2.5,
    5.0,
    float("inf"),
)
