--Analysing video games sales data set
--2.a. How many games have been released with 3 or more platforms?
--Creating a table showing number of platforms per game.
WITH num_of_platform
AS (
    SELECT name
        ,COUNT(DISTINCT platform) AS number_of_platform
    FROM sales_and_ratings
    GROUP BY name
    )
    --Choosing number of platform grater or equal to 3.
    ,num_of_platform_3_and_above
AS (
    SELECT *
    FROM num_of_platform
    WHERE number_of_platform >= 3
    )
--And counting the results.
SELECT COUNT(*)
FROM num_of_platform_3_and_above;

--2.b. Which year had the biggest amount of genres?
--We will start with claculating genres by year.
WITH genre_by_year
AS (
    SELECT Year_of_Release
        ,Genre
        ,COUNT(genre) AS num_of_genre
    FROM sales_and_ratings
    WHERE Year_of_Release IS NOT NULL
        AND Genre IS NOT NULL
    GROUP BY Year_of_Release
        ,Genre
    )
    --Ranking the number of genres in descending order.
    ,rn_by_genre
AS (
    SELECT *
        ,ROW_NUMBER() OVER (
            PARTITION BY genre ORDER BY num_of_genre DESC
            ) AS rn
    FROM genre_by_year
    )
--Choose rn 1, meaning genres that peaked and the years they peaked in.
--Then group by year of release and count how many genres peaked at the same year. Choose the year with the most genres that peaked.
SELECT TOP 1 Year_of_Release
    ,COUNT(*) AS genres_that_peaked
FROM rn_by_genre
WHERE rn = 1
GROUP BY Year_of_Release
ORDER BY genres_that_peaked DESC;

--3.
--Calculating the average of critic score per rating.
WITH average
AS (
    SELECT Rating
        ,ROUND(SUM(critic_score) / COUNT(critic_score), 1) AS avg_critic_score
        ,ROUND(SUM(Critic_Score * Critic_Count) / SUM(critic_count), 1) AS critic_score_weighted_average
    FROM sales_and_ratings
    WHERE Critic_Score IS NOT NULL
    GROUP BY Rating
    )
    --Calculating the mode for every rating
    ,mode
AS (
    SELECT Rating
        ,Critic_Score AS critic_score_mode
        ,COUNT(critic_count) AS critic_count
    FROM sales_and_ratings
    WHERE Critic_Score IS NOT NULL
    GROUP BY Rating
        ,Critic_Score
    )
    --Ranking count in descending order
    ,mode_rank
AS (
    SELECT *
        ,DENSE_RANK() OVER (
            PARTITION BY rating ORDER BY critic_count DESC
            ) AS rn
    FROM mode
    )
--Choosing the highest count for every rating
SELECT mr.Rating
    ,a.avg_critic_score
    ,a.critic_score_weighted_average
    ,mr.critic_score_mode
FROM mode_rank mr
LEFT JOIN average a ON a.Rating = mr.Rating
WHERE rn = 1;

--4. Year over year "YOY" growth analysis.
--First, creating table with sum of global sales for genre and platform (in group by).
--We will also create a lead on year to start creating the date scaffolding.
--Notice the third item in the lead : year_of_release +1 . Was added to include the last year in the partition.
WITH release_dates
AS (
    SELECT Genre
        ,Platform
        ,Year_of_release
        ,SUM(Global_Sales) AS total_global_sales
        ,LEAD(Year_of_Release, 1, Year_of_Release + 1) OVER (
            PARTITION BY genre
            ,platform ORDER BY year_of_release
            ) AS lead_year
    FROM Sales_and_Ratings
    WHERE Genre IS NOT NULL
        AND Year_of_Release IS NOT NULL
    GROUP BY Genre
        ,Platform
        ,Year_of_release
    )
    --Creating a full timeline
    ,all_dates
AS (
    SELECT DISTINCT Year_of_Release
    FROM sales_and_ratings
    WHERE Year_of_Release IS NOT NULL
    )
--Joining the two cte's. including total global sales null as 0.
SELECT rd.Genre
    ,rd.Platform
    ,ad.Year_of_Release
    ,CASE 
        WHEN ad.Year_of_Release <> rd.Year_of_Release
            THEN 0
        ELSE rd.total_global_sales
        END 'globalsales'
FROM all_dates ad
LEFT JOIN release_dates rd ON ad.Year_of_Release >= rd.Year_of_Release
    AND ad.Year_of_Release < rd.lead_year
WHERE rd.Genre IS NOT NULL
    AND ad.Year_of_Release IS NOT NULL
ORDER BY rd.Genre
    ,rd.Platform
    ,ad.Year_of_Release;

--5. 
--We will start where we ended ex. 4 (excluding genre):
WITH release_dates
AS (
    SELECT PLATFORM
        ,Year_of_release
        ,SUM(Global_Sales) AS total_global_sales
        ,LEAD(Year_of_Release, 1, Year_of_Release + 1) OVER (
            PARTITION BY platform ORDER BY year_of_release
            ) AS lead_year
    FROM Sales_and_Ratings
    WHERE Year_of_Release IS NOT NULL
    GROUP BY Platform
        ,Year_of_release
    )
    --Creating a timeline.
    ,all_dates
AS (
    SELECT DISTINCT Year_of_Release
    FROM sales_and_ratings
    WHERE Year_of_Release IS NOT NULL
    )
    --Performing the date scaffolding between the two cte's.
    ,sales_scaffolded
AS (
    SELECT rd.Platform
        ,ad.Year_of_Release
        ,CASE 
            WHEN ad.Year_of_Release <> rd.Year_of_Release
                THEN 0
            ELSE rd.total_global_sales
            END 'globalsales'
    FROM all_dates ad
    LEFT JOIN release_dates rd ON ad.Year_of_Release >= rd.Year_of_Release
        AND ad.Year_of_Release < rd.lead_year
    WHERE ad.Year_of_Release IS NOT NULL
    )
    --Creating a lag on globalsales to show last year.
    ,lag_global_sales
AS (
    SELECT *
        ,LAG(globalsales, 1, NULL) OVER (
            PARTITION BY platform ORDER BY year_of_release
            ) AS last_year_global_sales
    FROM sales_scaffolded
    )
    --Removing null values, and instances where last_year_global_sales = 0 (so that we dont divide by 0).
    --Why remove last_year_global_sales = o: there is 0 because there is missing information in the data about that year, we can't do the analysis regarding this year.
    --Why remove nulls: null regards to the 1st year in the partition - platform. it has no YOY value.
    --Will leave instances where global sales = 0 and last year global sales > 0 . because if there was a decrease in sales- we want to document that.
    ,yoy_growth
AS (
    SELECT *
        ,((globalsales - last_year_global_sales) / last_year_global_sales) * 100 AS YOY_growth_percent
    FROM lag_global_sales
    WHERE last_year_global_sales IS NOT NULL
        AND last_year_global_sales > 0
    )
    --Create rank by yoy desc.				
    ,rn_growth_percent
AS (
    SELECT *
        ,DENSE_RANK() OVER (
            PARTITION BY Platform ORDER BY yoy_growth_percent DESC
            ) AS rn
    FROM yoy_growth
    )
--Final yoy analysis per platform - the year with the highest YOY per platform.
SELECT Platform
    ,Year_of_Release
    ,ROUND(YOY_growth_percent, 2) AS yoy_growth_percentage
FROM rn_growth_percent
WHERE rn = 1
ORDER BY Platform
    ,Year_of_Release
    ,YOY_growth_percent DESC;
