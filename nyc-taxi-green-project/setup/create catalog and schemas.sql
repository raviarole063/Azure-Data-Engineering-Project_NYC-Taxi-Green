-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS nyctaxi;

-- COMMAND ----------

USE CATALOG nyctaxi;

CREATE SCHEMA IF NOT EXISTS 00_landing;
CREATE SCHEMA IF NOT EXISTS 01_bronze;
CREATE SCHEMA IF NOT EXISTS 02_silver;
CREATE SCHEMA IF NOT EXISTS 03_gold;

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS nyctaxi.00_landing.data_sources
LOCATION 'abfss://landing@stnyctaxigreen.dfs.core.windows.net/';