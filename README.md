# 📊 Assignment for Data Engineer

**Problem Statement :** 
Real-Time Cryptocurrency Data Pipeline Using GCP and Python

A digital marketing agency wants to monitor and analyze real-time cryptocurrency data to identify market trends and support datadriven strategy decisions. The goal is to design an automated data pipeline that extracts pricing data from the dynamic website https://www.investing.com/crypto/ for top 10 crypto currencies, processes it, and makes it available for analysis and visualization through a dashboard.

As a data engineer, your task is to build a real-time data pipeline using Google Cloud Platform (GCP) and Python. The pipeline should begin with a Selenium script that scrapes key cryptocurrency data—including name, symbol, current price, % change, volume, and market cap—from the specified website. This scraping process should run at hourly intervals.

Once the data is scraped, process and clean it using Pandas and NumPy. This includes handling missing values, converting data types, and calculating additional metrics such as percentage change, z-scores, and rolling averages.

After processing, the data should be published to a Google Cloud Pub/Sub topic. A Cloud Function, triggered by that topic, should parse the incoming data and store it in a BigQuery table. To automate the scraping process, set up a Cloud Scheduler job to trigger the Selenium script at regular intervals (every 60 minutes).

Finally, build a Looker Studio dashboard connected to the BigQuery table. The dashboard should include filters such as
cryptocurrency name/symbol, time range, and percentage change thresholds, along with visualizations like time-series charts, key
performance indicators, and a dynamic summary table to highlight market movements.

---
