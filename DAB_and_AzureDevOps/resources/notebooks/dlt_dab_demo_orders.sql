-- Databricks notebook source
-- create streaming table
CREATE OR REFRESH STREAMING TABLE st_orders
AS
SELECT * FROM STREAM(samples.tpch.orders)

-- COMMAND ----------

-- create MV
CREATE OR REFRESH MATERIALIZED VIEW agg_orders
AS
SELECT 
count(o_orderkey) as cnt_orders,
o_orderstatus 
FROM LIVE.st_orders
group by o_orderstatus
