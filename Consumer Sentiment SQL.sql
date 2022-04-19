-- Business conditions not included in final project 
CREATE TABLE business_conditions (
    "date" DATE,
    cont_inc REAL,
    inter_inc REAL,
    unchanged REAL,
    inter_dec REAL,
    cont_dec REAL,
    mix_change REAL,
    DK_NA REAL,
    "relative" REAL
);

ALTER TABLE business_conditions 
ADD CONSTRAINT pkey PRIMARY KEY ("date");
UPDATE business_conditions 
SET total = cont_inc + inter_inc + unchanged + inter_dec + cont_dec + mix_change + DK_NA;

COPY business_conditions
FROM '/Users/jackboydell/Desktop/CS 475/Consumer Sentiment Project/UMICH-SOC27.csv'
WITH (FORMAT CSV, HEADER);

COPY (SELECT "date", cont_inc, inter_inc, unchanged, inter_dec, cont_dec, mix_change, DK_NA,
cont_inc / total * 100 as cont_inc_perc, inter_inc / total * 100 as inter_inc_perc, 
unchanged / total * 100 as unchanged_perc, inter_dec / total * 100 as inter_dec_perc, 
cont_dec / total * 100 as cont_dec_perc, mix_change / total * 100 as mix_change_perc,
DK_NA / total * 100 as DK_NA_perc, total 
FROM business_conditions)
TO '/Users/jackboydell/Desktop/CS 475/Consumer Sentiment Project/UMICH-SOC27-first-look.csv'
WITH (FORMAT CSV, HEADER);

--------------------------------------------------------------------------------------------------------------
CREATE TABLE CS_unemployment(
    "date" DATE,
    go_up REAL,
    stay_same REAL,
    go_down REAL,
    DK_NA REAL,
    "relative" REAL
);
COPY CS_unemployment 
FROM '/Users/jackboydell/Desktop/CS 475/Consumer Sentiment Project/UMICH-SOC30-unemployment.csv'
WITH (FORMAT CSV, HEADER);

CREATE TABLE unemployment_rates (
    series_id TEXT,
    "year" INT,
    "month" CHAR(3),
    unemployment_rate REAL,
    one_month_net_change REAL,
    twelve_month_net_change REAL,
    one_month_percent_change REAL,
    twelve_month_percent_change REAL
);
COPY unemployment_rates 
FROM '/Users/jackboydell/Desktop/CS 475/Consumer Sentiment Project/Unemployment Rates .csv'
WITH (FORMAT CSV, HEADER);

ALTER TABLE unemployment_rates DROP COLUMN series_id;

COPY (SELECT C.date, C.year, C.month, go_up, stay_same, go_down, DK_NA, "relative", M.year, M.month,
    unemployment_rate, one_month_net_change, twelve_month_net_change, one_month_percent_change, twelve_month_percent_change
FROM (SELECT date, date_part('year', date) as year, date_part('month', date) as month, go_up, stay_same,
    go_down, DK_NA, "relative" 
FROM cs_unemployment) as C JOIN 
(SELECT year, substring("month" FROM '\d{2}')::INT as month, unemployment_rate, one_month_net_change, twelve_month_net_change,
    one_month_percent_change, twelve_month_percent_change
FROM unemployment_rates) as M
ON M.year = (C.year + 1) AND C.month = M.month)
TO '/Users/jackboydell/Desktop/CS 475/Consumer Sentiment Project/unemployment_rates_final.csv'
WITH (FORMAT CSV, HEADER);

----------------------------------------------------------------------------------------------------------------
CREATE TABLE change_in_interest_rates_MICH (
    "date" DATE,
    go_up REAL,
    stay_same REAL,
    go_down REAL,
    DK_NA REAL,
    "relative" REAL
);
COPY change_in_interest_rates_MICH 
FROM '/Users/jackboydell/Desktop/CS 475/Consumer Sentiment Project/Change_in_interest_rates.csv'
WITH (FORMAT CSV, HEADER);

CREATE TABLE interest_rates (
    id SERIAL,
    "date" DATE,
    fed_funds_rate REAL
);

COPY interest_rates (date, fed_funds_rate)
FROM '/Users/jackboydell/Desktop/CS 475/Consumer Sentiment Project/FederalFundsRate.csv'
WITH (FORMAT CSV, HEADER);

COPY (SELECT M.date, M.year, M.month, M.go_up, M.stay_same, M.go_down, M.dk_na, M.relative, F.year, F.fed_funds_rate
FROM 
(SELECT date, date_part('year', date) as "year", date_part('month', date) as "month", go_up, stay_same, go_down, dk_na, "relative"
    FROM change_in_interest_rates_MICH)  as M 
    JOIN 
    (SELECT date_part('year', date) as "year", date_part('month', date) as "month", fed_funds_rate
    FROM interest_rates) as F
ON F.year = (M.year) AND M.month = F.month)
TO '/Users/jackboydell/Desktop/CS 475/Consumer Sentiment Project/interest_rates_final_DAVID.csv'
WITH (FORMAT CSV, HEADER); -- doesn't have change in fed_funds_rate per month yet

-- interesting note: CTEs WITH statements seem to need to be followed by a SELECT statement only
-- cannot CREATE TABLE or COPY right after in same query 
