# CustomerAnalysisBigData
[![Spark](https://img.shields.io/badge/Spark-3.5.1-orange)](https://spark.apache.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.4.2-blue)](https://dev.mysql.com/doc/)


## Description

This project focuses on customer behavior analysis for smart TV users, aiming to understand their content preferences over time. By analyzing viewing and search data, the project tracks category trends, highlighting how customers' tastes evolve month by month. It also identifies the most searched categories each month, providing insights into popular content and user engagement patterns.

**CATEGORY lABELS:** `Action`, `Anime`, `C-Drama`, `Comedy`, `Horror`, `K-Drama`, `Kids`, `Music`, `News`, `Romantic`, `Show`, `Sports`, `TL-Drama`, `V-Drama`.

## Processing data
* Objective: Process, ETL raw data into fact data that can analyzed.
* Raw data are stored by parquet files that have been prepared in folder `log search`. With the data schema as below:

```python
root
 |-- eventID: string (nullable = true)
 |-- datetime: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- keyword: string (nullable = true)
 |-- category: string (nullable = true)
 |-- proxy_isp: string (nullable = true)
 |-- platform: string (nullable = true)
 |-- networkType: string (nullable = true)
 |-- action: string (nullable = true)
 |-- userPlansMap: array (nullable = true)
 |    |-- element: string (containsNull = true)
```
* The process focuses on `datetime`, `user_id`, and `keyword`. For `keyword`, users may search using variations like `trữ tình`, `trử tình`, or `    trữ tình`—all representing the same context but stored differently. To handle this, several preprocessing steps are required, such as removing extra spaces, accents, and (optionally) performing spell checks before mapping the keywords to the appropriate category.

* The output of ETL processing will be analyzable data. After the ETL process, two key tables will be generated: "Most_Category_Search", which tracks the most popular categories for each user, and "Analysis_Customer_Taste", which monitors the changes in user category preferences over time, month by month.

* `Most_Category_Search` table:

user_id|category_6|category_7|
---|---|---|
0008207|Anime|Kids|
0003691|Action|Action|
0041173|K-Drama|Kids|
0028736|Anime|Anime|
0025492|K-Drama|K-Drama|

* `Analysis_Customer_Taste` table:

user_id|category_6|category_7|Trending_type|Previous|
---|---|---|---|---|
0008207|Anime|Kids|Changed|Anime-Kids|
0003691|Action|Action|Unchanged|Unchanged|
0041173|K-Drama|Kids|Changed|K-Drama-Kids|
0028736|Anime|Anime|Unchanged|Unchanged|
0025492|K-Drama|K-Drama|Unchanged|Unchanged|
## Visual and analytical

Use [Power BI](https://app.powerbi.com/) to visualize data

![all_text](./images/image.png)

**Growth Rate by Month (June vs. July):**

* `Kids` category experienced the highest growth rate **(85.50%)** from 2,655 views in **June** to 4,925 in **July**.
* `News` saw a notable increase **(65.71%)** from 210 views in **June** to 348 in **July**.
`C-Drama` grew by **17.77%**, from 21,886 to 25,776 views.
Categories such as `Horror` **(-39.39%)**, `Comedy` **(-23.98%)**, and `K-Drama` **(-15.92%)** witnessed significant declines in viewership.

**Change Ratio:**

* **94.73%** of user preferences remained unchanged, while **5.27%** changed, indicating that a small portion of the audience is shifting their interests.

**Growth Rate by Category:**

* The heatmap reflects shifts in customer viewing behavior across different genres, with `C-Drama` and `K-Drama` showing the highest engagement levels, particularly in specific subcategories like `Kids` and `Shows`.
* Overall, this analysis highlights the dynamic shifts in viewer interests across genres, providing insights into trending and declining categories for further content strategy planning.

🔥🔥🔥 🤝🤝🤝 🔥🔥🔥
