CREATE OR REFRESH STREAMING TABLE elt.bank_txns_bronze_2
COMMENT "Bronze layer of bank txns"
TBLPROPERTIES ("quality"="bronze")
AS
SELECT
  *,
  current_timestamp() AS ingestion_ts
FROM STREAM(databricks_practice.inputdb.bank_txns);

CREATE OR REFRESH LIVE TABLE elt.bank_txns_silver
COMMENT "Silver layer of bank txns"
TBLPROPERTIES ("quality"="silver")
AS
SELECT
  txn_id,
  cust_id,
  payment_type,
  CAST(amount AS DECIMAL(18,2)) AS amount,
  CAST(txn_date AS DATE)        AS txn_date,
  ingestion_ts
FROM LIVE.elt.bank_txns_bronze_2
WHERE
  amount IS NOT NULL
  AND amount > 0
  AND cust_id IS NOT NULL
  AND txn_date IS NOT NULL;

CREATE OR REFRESH LIVE TABLE elt.bank_txns_gold
COMMENT "Gold layer - daily account transaction summary"
TBLPROPERTIES (
  "quality" = "gold"
)
AS
SELECT
  cust_id,
  txn_date,
  COUNT(*)    AS total_txns,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_txn_amount
FROM LIVE.elt.bank_txns_silver
GROUP BY cust_id, txn_date;