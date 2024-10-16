# databricks-auto-trading
> Date & Time:  2024-10-15 17:38:01
> Generated with the help of PLAUD

## Overview
The following document outlines the meeting notes from a discussion held on 2024-10-15 regarding an automated trading algorithm  project. The project involves creating a personal automated stock trading algorithm hosted in Databricks and AWS. Key topics include the investment strategy, strategy implementation, data engineering, and hypotheses for trading strategies.

### High-Level Description
* **Project Overview**: This is a personal automated stock trading algorithm, hosted in Databricks and AWS.
* **Investment Strategy**: 
  * Invest a fixed amount daily (e.g., $100/day).
  * Diversify the portfolio to maintain several trading strategies simultaneously.
  * Initial strategies include:
    * Random selection.
    * Machine learning algorithm to predict stock prices or estimate undervalued/overvalued stocks.
    * Potential third strategy using sentiment data for short-term trading.
    * Potential cash position.
### Strategy Implementation
* **Allocation of Funds**:
  * Distribute the daily investment amount across different strategies.
  * Allocation based on prior returns, modeled as a Laplace distribution with skew.
  * Estimate the probability of return and weight the investment towards strategies with higher probabilities based on past returns.
### Data Engineering
* **Data Sources**:
  * **Alpaca Trade API**: Ingesting stock performance data, tickers, and other general information.
  * **FRED St. Louis Federal Reserve Database**: Quarterly estimates of unemployment, CPI, and GDP.
  * **Y Finance Python Package**: Balance sheet data, cash flow, and income sheets on a quarterly basis per ticker symbol.
### Hypotheses and Strategies
* **Random Selection Hypothesis**:
  * Focus on penny stock trading.
  * Filter for last prices from trading day being less than $5.
  * Further filter using quarterly financial data for penny stocks with reasonable EPS and other business value metrics.
  * Random selection applied after filtering for appropriate price per stock and EPS.
* **Machine Learning Hypothesis**:
  * Merge FRED data, prior stock history data, and quarterly financial data.
  * Exclude social media sentiment data initially.
  * Lag data for five trading days to predict ahead of time.
  * Use regression errors (overestimating or underestimating) to inform trading strategy.
  * Decide on trading frequency (daily or weekly).

> **AI Suggestion**
> AI has identified the following issues that were not concluded in the meeting or lack clear action items; please pay attention:
> 1. **Strategy Implementation**: Decide on the trading frequency for the machine learning strategy (daily or weekly).
> 2. **Data Engineering**: Clarify the specific roles and responsibilities for ingesting and processing data from Alpaca Trade API, FRED St. Louis Federal Reserve Database, and Y Finance Python Package.
> 3. **Hypotheses and Strategies**: Define the exact criteria for filtering penny stocks based on EPS and other business value metrics.
> 4. **Data Engineering**: Potential delays in data ingestion and processing due to unclear responsibilities and roles.
> 5. **Hypotheses and Strategies**: Risk of ineffective trading strategies if the criteria for filtering penny stocks are not well-defined.

## Action Items
### Project Overview and Strategy Development
- [ ]  Finalize the high-level description of the auto trading project.
  * **Notes**: Title should be "Auto Trading". This is a personal automated stock trading algorithm hosted in Databricks and AWS.
- [ ]  Develop and document the three trading strategies.
  * **Notes**: 
    * Strategy 1: Random selection.
    * Strategy 2: Machine learning algorithm to predict stock prices or estimate undervalued/overvalued stocks.
    * Strategy 3: Using sentiment data for short-term trading.
- [ ]  Determine the allocation of daily investment ($100) across the different strategies based on prior returns.
  * **Notes**: Model this as a Laplacian distribution with skew to estimate the probability of return and weight the investment accordingly.
### Data Engineering and Ingestion
- [ ]  Ingest stock performance data from the Alpaca API.
  * **Notes**: Use the alpaca_trade_api package to gather stock performance data, tickers, and other general information.
- [ ]  Ingest quarterly economic data from the FRED St. Louis Federal Reserve database.
  * **Notes**: Collect quarterly estimates of unemployment, CPI, and GDP.
- [ ]  Ingest financial data from the Yahoo Finance Python package.
  * **Notes**: Gather balance sheet data, cash flow, and income sheets on a quarterly basis per ticker symbol.
- [ ]  Merge all ingested data for use in machine learning and random hypothesis testing.
  * **Notes**: Combine FRED data, prior stock history data, and quarterly financial data.
### Hypothesis Testing and Filtering
- [ ]  Implement the random hypothesis for penny stock trading.
  * **Notes**: 
    * Filter for last prices from trading day being less than $5.
    * Further filter for penny stocks with reasonable EPS and other basic business value metrics.
- [ ]  Develop the machine learning model to predict stock prices.
  * **Notes**: 
    * Merge FRED data, prior stock history data, and quarterly financial data.
    * Lag data for five trading days to predict ahead of time.
    * Use regression errors to inform trading strategy.
### Decision Making and Implementation
- [ ]  Decide on the frequency of trading (daily or weekly) based on machine learning predictions.
  * **Notes**: Use regression errors to determine whether to buy on a daily or weekly basis.
