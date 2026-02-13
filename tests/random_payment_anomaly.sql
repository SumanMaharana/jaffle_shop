-- Singular test: Random payment anomaly detection
-- Simulates an anomaly detection algorithm with varying sensitivity
--
-- Test outcomes based on random threshold:
-- - High threshold (>0.8): Likely to PASS (few/no anomalies detected)
-- - Medium threshold (0.4-0.8): Likely to WARN (some anomalies)
-- - Low threshold (<0.4): Likely to FAIL (many anomalies)

with random_threshold as (
    select random() as detection_threshold
),

payment_analysis as (
    select
        o.order_id,
        o.amount,
        o.credit_card_amount,
        o.bank_transfer_amount,
        o.coupon_amount,
        o.gift_card_amount,
        random() as anomaly_score,
        (select detection_threshold from random_threshold) as threshold
    from {{ ref('orders') }} o
)

select
    order_id,
    amount,
    anomaly_score,
    threshold,
    'Anomaly detected! Score: ' || round(anomaly_score::numeric, 3) || ' | Threshold: ' || round(threshold::numeric, 3) as details,
    case
        when anomaly_score < threshold * 0.5 then 'CRITICAL_ANOMALY'
        when anomaly_score < threshold then 'SUSPECTED_ANOMALY'
        else 'PASSED'
    end as severity_level
from payment_analysis
where anomaly_score < threshold
order by anomaly_score

-- Returns varying number of "anomalous" payments each run
-- Simulates real-world ML model behavior with probabilistic results
