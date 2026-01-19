## Monitoring Reservoir Storage using Remote Sensing and Large Language Models

This repository contains two R scripts that together (1) generate satellite-based reservoir storage estimates from Sentinel-1 SAR imagery and (2) automatically mine public news articles for reported storage values using an LLM.

### 1) Remote sensing workflow (Sentinel-1 -> water area -> storage -> validation)

A Quarto/R analysis script that:

* Authenticates to Google Earth Engine and Google Cloud Storage, downloads and caches Sentinel-1 VV imagery for the Aposelemis reservoir ROI.
* Applies speckle reduction (median filtering) and threshold-based water masking across a grid of parameters (threshold **T** [dB], filter radius **r** [m]).
* Converts water surface area to storage using tabulated area–elevation–storage curves.
* Compares satellite-derived storage to observed storage and evaluates performance (RMSE, MAE, R<sup>2</sup>, KGE), including uncertainty via circular moving-block bootstrap.
* Produces publication-ready figures and tables in `figures_out/` and stores intermediate artifacts in `artifacts/`.

### 2) Aposelemis news miner (Google CSE + OpenAI → incremental CSV)

A standalone R script that:

* Uses Google Custom Search (JSON API) to discover Greek webpages mentioning Αποσελέμης along with storage-related terms.
* Downloads new articles (skipping URLs already logged), extracts readable text (with AMP fallback), and calls the OpenAI Responses API with a strict JSON schema.
* Extracts an **as-of date** and **storage** value (converted to hm<sup>3</sup>) and appends/updates an output CSV (e.g., `aposelemis_mined_storage.csv`) with columns: `Date`, `Storage`, `Source`.
* Supports incremental re-runs and year-by-year mining to manage quotas and rate limits.

### Notes

Both scripts rely on external API credentials (Earth Engine / Google Cloud / Google CSE / OpenAI) loaded from local `*_api_keys.R` files or environment variables.
