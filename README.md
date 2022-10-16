# Generic Buy Now, Pay Later Project

**Group details:**
- Group: 27
- Tutor: Lucas Fern
- Group members:
    - Yi Fan (Lily) Li 1171494
    - Menghan Dong (Catherine) 1174200
    - Huafang Mai 1173936
    - Yufeng (Tony) Xie 1166106
    - Baichuan (George) Yu 1174260

**Research Goal:** Our research goal is to create a ranking system to determine the top 100 merchants for our Buy Now Pay Later company to onboard.

**Timeline:** The timeline for the research area is 2021 March - 2022 October.

**Summary:** See `notebooks/summary.ipynb` for a quick summary of this project.

**Code:**
To run the pipeline, please run the following files in order:
1. `python3 scripts/etl.py data/tables data/curated/etl_tables`: This processes all the internal raw data (merchant, customer and transaction) and joins them together. Curated data is saved to the `data/curated/etl_tables` directory.
2. `notebooks/fraud.ipynb`: This notebook assigns fraud group lables to each transaction. The output is saved in `data/curated/merchant_consumer_info.parquet`. 
3. `notebooks/pobox_postcode.ipynb`: This notebook finds a match for most of the postcodes that do not match up with the postcode shapefile. The output is saved in the `data/meta/` directory.
4. `notebooks/map_overlay.ipynb`: This notebook matches postcodes and sa2 codes based on their respective shapefiles and joins the external and internal datasets. The output is saved in `data/curated/merchant_consumer_abs.parquet`.
5. `notebooks/shapefile.ipynb` (optional): This notebook produces maps the distribution of sa2 data over the different areas of Australia.
6. `notebooks/trainRevenue.ipynb` (optional): This notebook demonstrates the training process with existing data. Plots generated are saved in the `plots/` directory.
7. `notebooks/predictRevenue.ipynb`: This notebook predicts revenue for November 2022 - December 2023. Predictions are saved in `data/curated/predictions.parquet`.
8. `notebooks/Aggregate_ranking_table.ipynb`: This notebook generates the standardised scores for each of our metrics based on the predictions. Scores are saved in `data/curated/score_table`.
8. `notebooks/rank.ipynb`: This notebook produces the final rankings based on the scores. Final rankings are saved in `data/result`.

**External data:**
All external data should be avaliable in the `data/ABS` directory. If not, then please download and store in the following way:
```
-- data
    -- ABS
        -- post_shapefile
            -- ...
        -- SA2 shapefile
            -- ...
        -- annual income by SA2 regions.xlsx
        -- population estimates by SA2 regions.xlsx
```
The data can be downloaded using the following links:
- post_shapefile: https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/POA_2021_AUST_GDA2020_SHP.zip
- SA2 shapefile: https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip
- annual income by SA2 regions.xlsx: https://www.abs.gov.au/statistics/labour/earnings-and-working-conditions/personal-income-australia/2014-15-2018-19/6524055002_DO001.xlsx
    - Only retain the sheet named 'Table 1.4'
- population estimates by SA2 regions.xlsx: https://www.abs.gov.au/statistics/people/population/regional-population/2021/32180DS0001_2001-21.xlsx
    - Only retain the sheet named 'Table 1'
    - Remove the first 6 rows from the table