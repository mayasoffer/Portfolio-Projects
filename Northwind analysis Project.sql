--1. Calculating customer satisfaction using Northwind DB.
WITH div_by_num_orders
AS (
    SELECT CustomerID
        ,OrderDate
        ,shipcountry
        ,lead(orderdate, 1, NULL) OVER (
            PARTITION BY customerid ORDER BY orderdate
            ) AS next_order
        ,ROW_NUMBER() OVER (
            PARTITION BY customerid ORDER BY orderdate
            ) AS num_of_order
    FROM orders
    )
    ,calc_date_diff
AS (
    SELECT *
        ,DATEDIFF(dd, OrderDate, next_order) AS date_diff
    FROM div_by_num_orders
    WHERE num_of_order = 1
    )
SELECT shipcountry
    ,AVG(date_diff) AS avg_date_diff_between_1st_and_2nd_order
    ,COUNT(customerid) AS unique_customer_count
FROM calc_date_diff
GROUP BY shipcountry;

--2. RFM model using Northwind DB.
WITH calculating_cols
AS (
    SELECT CustomerID
        ,MAX(OrderDate) AS last_order_date
        ,COUNT(o.OrderID) AS order_count
        ,ROUND(SUM(od.ProductID * UnitPrice * Quantity * (1 - Discount)), 1) AS total_cost_of_all_orders
    FROM Orders o
    JOIN [Order Details] od ON o.OrderID = od.OrderID
    GROUP BY customerid
    )
    ,calculating_rfm_cols
AS (
    SELECT *
        ,(
            NTILE(5) OVER (
                ORDER BY last_order_date
                )
            ) * 100 AS recency
        ,(
            NTILE(5) OVER (
                ORDER BY order_count
                )
            ) * 10 AS frequency
        ,NTILE(5) OVER (
            ORDER BY total_cost_of_all_orders
            ) AS monetary
    FROM calculating_cols
    )
SELECT CustomerID
    ,recency + frequency + monetary AS RFM
FROM calculating_rfm_cols
ORDER BY RFM DESC;

--3. Pareto Analysis:
--Are 20% of customers responsible for 80% of purchases? let's check that for the NorthwindDB.
--[Sometimes ranges as 30-70].
WITH total_purchase_per_customerid
AS (
    SELECT CustomerID
        ,SUM(unitprice * Quantity * (1 - Discount)) AS total_purchased
    FROM Orders o
    JOIN [Order Details] od ON o.OrderID = od.OrderID
    GROUP BY CustomerID
    )
    ,cumulative_purchases
AS (
    SELECT customerid
        ,COUNT(customerid) OVER (
            ORDER BY total_purchased DESC
            ) AS a
        ,COUNT(customerid) OVER () AS b
        ,SUM(total_purchased) OVER (
            ORDER BY total_purchased DESC
            ) AS cumulative_total_purchased
        ,SUM(total_purchased) OVER () AS all_time_purchases
    FROM total_purchase_per_customerid
    )
SELECT ROUND((CAST(a AS FLOAT) / CAST(b AS FLOAT)) * 100, 2) AS customers_ration_percent
    ,ROUND((cumulative_total_purchased / all_time_purchases) * 100, 2) AS sales_ration_percanteges
FROM cumulative_purchases
    --For conclusion- in this instance, 29.12% of customers are responsible for 71.21% of sales.
    ;

--4. Customer aquisition analysis using Northwind DB.
WITH getting_first_customer_furchase_date
AS (
    SELECT shipcountry
        ,customerid
        ,FORMAT(MIN(orderdate), 'yyyy-MM-01') AS first_order_date
    FROM Orders
    GROUP BY shipcountry
        ,CustomerID
    )
    ,switching_to_continent
AS (
    SELECT *
        ,CASE 
            WHEN shipcountry IN (
                    'Argentina'
                    ,'Venezuela'
                    ,'Brazil'
                    )
                THEN 'South America'
            WHEN shipcountry IN (
                    'Mexico'
                    ,'USA'
                    ,'Canada'
                    )
                THEN 'North America'
            WHEN shipcountry IN (
                    'Denmark'
                    ,'Finland'
                    ,'Norway'
                    ,'Sweden'
                    )
                THEN 'Scandinavia'
            WHEN shipcountry IN (
                    'France'
                    ,'Austria'
                    ,'Belgium'
                    ,'Germany'
                    ,'Ireland'
                    ,'Italy'
                    ,'Poland'
                    ,'Portugal'
                    ,'Spain'
                    ,'Switzerland'
                    ,'UK'
                    )
                THEN 'Europe'
            END AS Continent
    FROM getting_first_customer_furchase_date
    )
    ,getting_num_of_new_customers_per_date_and_continent
AS (
    SELECT continent
        ,first_order_date
        ,COUNT(CustomerID) AS num_of_new_customers
    FROM switching_to_continent
    GROUP BY continent
        ,first_order_date
    )
    ,dates
AS (
    SELECT DISTINCT FORMAT(orderdate, 'yyyy-MM-01') AS date_
    FROM Orders
    )
    ,lead_dates
AS (
    SELECT *
        ,LEAD(first_order_date, 1, '1998-06-01') OVER (
            PARTITION BY continent ORDER BY first_order_date
            ) AS lead_date
    FROM getting_num_of_new_customers_per_date_and_continent
    )
    ,scaffolding_data
AS (
    SELECT Continent
        ,date_
        ,CASE 
            WHEN ld.first_order_date <> date_
                THEN 0
            ELSE ld.num_of_new_customers
            END 'new_customers'
    FROM lead_dates ld
    LEFT JOIN dates d ON date_ >= first_order_date
        AND date_ < lead_date
    )
    ,lnc
AS (
    SELECT *
        ,Lag(new_customers, 1, NULL) OVER (
            PARTITION BY continent ORDER BY date_
            ) AS lag_new_customers
    FROM scaffolding_data
    )
    ,growth_classification
AS (
    SELECT *
        ,CASE 
            WHEN new_customers > lag_new_customers
                THEN 'growth'
            WHEN new_customers < lag_new_customers
                THEN 'decrease'
            WHEN new_customers = lag_new_customers
                THEN 'plateau'
            ELSE 'growth'
            END 'classification'
    FROM lnc
    )
SELECT continent
    ,classification
    ,COUNT(classification) AS classification_count
FROM growth_classification
GROUP BY Continent
    ,classification
ORDER BY continent
    ,classification
    --To conclude: europe is the continent with the biggest amount of years that had growth in number of new customers vs. the years prior.
    ;

--5. Football matches excercise- creating from an original table another table with the columns: scored, receives, goaldiff, points.
--Note- points will be calculated like this: 3 pts for a win, 1 for tie, 0 for loss.
--First creating the original table:
CREATE TABLE Matches (
    HomeTeam VARCHAR(100)
    ,AwayTeam VARCHAR(100)
    ,HomeScore INT
    ,AwayScore INT
    )

INSERT INTO Matches
VALUES (
    'Argentina'
    ,'Nigeria'
    ,2
    ,0
    )
    ,(
    'Germany'
    ,'Japan'
    ,1
    ,1
    )
    ,(
    'Japan'
    ,'Argentina'
    ,0
    ,1
    )
    ,(
    'Germany'
    ,'Nigeria'
    ,2
    ,3
    )
    ,(
    'Nigeria'
    ,'Japan'
    ,0
    ,0
    )
    ,(
    'Germany'
    ,'Argentina'
    ,1
    ,0
    )

SELECT *
FROM Matches;

WITH adding_points
AS (
    SELECT *
        ,CASE 
            WHEN HomeScore > AwayScore
                THEN 3
            WHEN HomeScore = AwayScore
                THEN 1
            ELSE 0
            END 'points_home'
        ,CASE 
            WHEN HomeScore < AwayScore
                THEN 3
            WHEN HomeScore = AwayScore
                THEN 1
            ELSE 0
            END 'points_away'
    FROM Matches
    )
    ,hometeam_pts
AS (
    SELECT hometeam
        ,SUM(homescore) AS scored_home
        ,SUM(awayscore) AS received_home
        ,SUM(points_home) AS pts_home
    FROM adding_points
    GROUP BY HomeTeam
    )
    ,awayteam_pts
AS (
    SELECT awayteam
        ,SUM(homescore) AS received_away
        ,SUM(awayscore) AS scored_away
        ,SUM(points_away) AS pts_away
    FROM adding_points
    GROUP BY AwayTeam
    )
    ,joining_the_two
AS (
    SELECT HomeTeam
        ,scored_home + ISNULL(scored_away, 0) AS scored
        ,received_home + ISNULL(received_away, 0) AS received
        ,pts_home + ISNULL(pts_away, 0) AS points
    FROM hometeam_pts hp
    LEFT JOIN awayteam_pts ap ON hp.HomeTeam = ap.AwayTeam
    )
SELECT *
    ,scored - received AS goaldiff
FROM joining_the_two;
