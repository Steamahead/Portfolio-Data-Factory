# Portfolio Data Factory

## Executive Summary
**Portfolio Data Factory** is a centralized Data Engineering ecosystem designed to build, automate, and monitor long-term datasets.

The core philosophy of this factory is **Long-Horizon Data Maturity**. The system is architected to run autonomously for **9-12 months**, collecting high-frequency market signals that are invisible in short-term windows. This allows for the creation of robust datasets for backtesting and predictive modeling, moving beyond simple "snapshot" analysis.

It serves as a serverless "laboratory" utilizing a modern tech stack (Cloud, AI, SQL, BI) to turn raw, unstructured noise into structured business intelligence.

---

## Project 1: Shiller Hybrid Index
*Status: Production Ready & Running Daily*

### 🛠 Tech Stack
![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Azure Functions](https://img.shields.io/badge/Azure%20Functions-Cloud-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Azure SQL](https://img.shields.io/badge/Azure%20SQL-Database-0078D4?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![Google Gemini](https://img.shields.io/badge/Google%20Gemini-LLM%20API-8E75B2?style=for-the-badge&logo=googlebard&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Analytics-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Git](https://img.shields.io/badge/Git-Version%20Control-F05032?style=for-the-badge&logo=git&logoColor=white)


<img width="2816" height="1536" alt="Gemini_Generated_Image_nlnyxlnlnyxlnlny" src="https://github.com/user-attachments/assets/8e3fa25e-cf7f-4a5e-b9b0-6203f91107dd" />


### Deep Dive Description

The **Shiller Hybrid Index** is an automated sentiment analysis system that addresses the problem of "Information Overload" in financial markets.

While traditional algorithms focus on price (Technical Analysis), this system quantifies **Narrative Economics** — measuring the psychological gap between market price and news sentiment. Every night at 22:30 UTC, the system orchestrates a serverless ETL pipeline:

1. **Orchestration & Ingestion:**
   The Python orchestrator triggers a parallel fetch of **Price Data** (Yahoo Finance) and **Global News** (NewsAPI) using a 3-Day Rolling Window to capture narrative momentum.

2. **Dual-Track Semantic Analysis:**
   The core of the system is a **Structured Analysis Framework** built on Gemini 2.5 Flash. Through advanced Prompt Engineering, the LLM acts as a gatekeeper, filtering signal from noise. 
   
   Each article is evaluated on two independent tracks:
   
   **Track A — Business Reality (Sentiment):**
   - *Materiality:* Differentiates "Market-Moving News" (Earnings, Regulation) from "Noise" (Op-eds, Clickbait)
   - *Source Credibility:* Bloomberg carries more weight than unverified blogs
   - *Recency:* Recent news weighted higher than older articles
   
   **Track B — Crowd Emotion (Hype):**
   - *Speculation Signal:* Measures emotional intensity — from "Cold/Factual" to "Mania/Delusion"
   - *Source Validity:* Reddit and Bloomberg equally valid for measuring crowd psychology
   
   **Quality-Weighted Aggregation:**
   Final scores use weighted averages where article quality determines influence. A high-quality Reuters report has more weight than a low-quality blog post.

3. **Divergence Detection:**
   The system visualizes the **Behavioral Gap** between price and fundamentals, testing a specific hypothesis: *Are investors buying the business (Sentiment) or the story (Hype)?*
   
   - **Healthy Market:** Price, Sentiment, and Hype move together
   - **Divergence Flag:** Price rises while Sentiment falls, or Hype is extreme while Materiality is low
   
   This flags scenarios where crowd emotion may have decoupled from business reality — useful for risk assessment and further analysis.

> ⚠️ **Disclaimer:** This is a portfolio project for educational purposes. It is NOT a trading system and does NOT provide investment advice. The system detects potential divergences for analysis — it does not predict market movements.

### 🏗 Architecture flow
```
graph LR
    A[🕒 Daily Trigger] -->|Orchestrator| B(🐍 Python / Azure Function)
    B -->|REST API| C[📰 News Aggregators & Market Data]
    B -->|Context & Scoring| D{"🧠 Gemini AI (Master Prompt)"}
    D -->|Structured Signals| E[(🗄️ Azure SQL Database)]
    E -->|Business Intelligence| F[📊 Power BI Dashboard]

