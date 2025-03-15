SELECT * FROM "Fintech_loans_clean"
ORDER by "Loan Amount" DESC
LIMIT 20;

==============================================================================
WITH Filtered_Lookup AS (
    SELECT "Original", "Imputed/Encoded" 
    FROM "Lookup_table"
    WHERE "Column" = 'State'
	)
SELECT flc."State",lt."Original" AS Decoded_State,
	AVG(flc."Annual Inc") AS Average_Income
FROM "Fintech_loans_clean" flc
JOIN Filtered_Lookup lt
ON flc."State"::TEXT = lt."Imputed/Encoded"
GROUP BY flc."State",lt."Original"
ORDER BY Average_Income DESC;

==============================================================================
WITH Filtered_Lookup AS (
    SELECT "Original", "Imputed/Encoded" 
    FROM "Lookup_table"
    WHERE "Column" = 'State'
	)
SELECT flc."State", lt."Original" AS Decoded_State, flc."Int Rate"
FROM "Fintech_loans_clean" flc
JOIN Filtered_Lookup lt
ON flc."State"::TEXT = lt."Imputed/Encoded"
GROUP BY flc."State",lt."Original",flc."Int Rate"
ORDER BY flc."Int Rate" DESC
LIMIT 1;

==============================================================================
WITH Filtered_Lookup AS (
    SELECT "Original", "Imputed/Encoded" 
    FROM "Lookup_table"
    WHERE "Column" = 'State'
	)
SELECT flc."State", lt."Original" AS Decoded_State, flc."Int Rate"
FROM "Fintech_loans_clean" flc
JOIN Filtered_Lookup lt
ON flc."State"::TEXT = lt."Imputed/Encoded"
GROUP BY flc."State",lt."Original",flc."Int Rate"
ORDER BY flc."Int Rate" 
LIMIT 1;

==============================================================================
WITH Filtered_Lookup AS (
    SELECT "Original", "Imputed/Encoded" 
    FROM "Lookup_table"
    WHERE "Column" = 'State'
	)
, Grade_Counts AS (
    SELECT lt."Original" AS Decoded_State, flc."Letter Grade" AS Grade, COUNT(flc."Letter Grade") AS Grade_Count
    FROM "Fintech_loans_clean" flc
    JOIN Filtered_Lookup lt
    ON flc."State"::TEXT = lt."Imputed/Encoded"
    GROUP BY lt."Original", flc."Letter Grade"
)
SELECT Decoded_State, Grade AS Most_Frequent_Grade, Grade_Count
FROM (
    SELECT Decoded_State, Grade, Grade_Count,
        ROW_NUMBER() OVER (PARTITION BY Decoded_State ORDER BY Grade_Count DESC) AS rank
    FROM Grade_Counts
) AS Ranked_Grades
WHERE rank = 1
ORDER BY Decoded_State;

==============================================================================
WITH Filtered_Lookup AS (
    SELECT "Original", "Imputed/Encoded" 
    FROM "Lookup_table"
    WHERE "Column" = 'State'
),
Filtered_df AS (
    SELECT *
	FROM "Fintech_loans_clean" f
    WHERE 
        (f."Loan Status_Charged Off" = 1 
        OR f."Loan Status_Late (16-30 days)" = 1
        OR f."Loan Status_Late (31-120 days)" = 1
        OR f."Loan Status_Default" = 1)
)
SELECT flc."State", lt."Original" AS Decoded_State, COUNT(*) AS non_paid_loans
FROM Filtered_df flc
JOIN Filtered_Lookup lt
ON flc."State"::TEXT = lt."Imputed/Encoded"
GROUP BY flc."State", lt."Original"
ORDER BY non_paid_loans DESC
LIMIT 1;

==============================================================================
SELECT 
    AVG(flc."Loan Amount") AS avg_loan_amount
FROM 
    "Fintech_loans_clean" flc
WHERE 
    flc."Issue Year" BETWEEN '2015' AND '2018';




