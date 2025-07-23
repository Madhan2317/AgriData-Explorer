CREATE TABLE agri_data (
    year INTEGER,
    state_name TEXT,
    dist_name TEXT,
    rice_area_1000_ha FLOAT,
    rice_production_1000_tons FLOAT,
    rice_yield_kg_per_ha FLOAT,
    wheat_area_1000_ha FLOAT,
    wheat_production_1000_tons FLOAT,
    wheat_yield_kg_per_ha FLOAT,
    kharif_sorghum_area_1000_ha FLOAT,
    kharif_sorghum_production_1000_tons FLOAT,
    kharif_sorghum_yield_kg_per_ha FLOAT,
    rabi_sorghum_area_1000_ha FLOAT,
    rabi_sorghum_production_1000_tons FLOAT,
    rabi_sorghum_yield_kg_per_ha FLOAT,
    sorghum_area_1000_ha FLOAT,
    sorghum_production_1000_tons FLOAT,
    sorghum_yield_kg_per_ha FLOAT,
    pearl_millet_area_1000_ha FLOAT,
    pearl_millet_production_1000_tons FLOAT,
    pearl_millet_yield_kg_per_ha FLOAT,
    maize_area_1000_ha FLOAT,
    maize_production_1000_tons FLOAT,
    maize_yield_kg_per_ha FLOAT,
    finger_millet_area_1000_ha FLOAT,
    finger_millet_production_1000_tons FLOAT,
    finger_millet_yield_kg_per_ha FLOAT,
    barley_area_1000_ha FLOAT,
    barley_production_1000_tons FLOAT,
    barley_yield_kg_per_ha FLOAT,
    chickpea_area_1000_ha FLOAT,
    chickpea_production_1000_tons FLOAT,
    chickpea_yield_kg_per_ha FLOAT,
    pigeonpea_area_1000_ha FLOAT,
    pigeonpea_production_1000_tons FLOAT,
    pigeonpea_yield_kg_per_ha FLOAT,
    minor_pulses_area_1000_ha FLOAT,
    minor_pulses_production_1000_tons FLOAT,
    minor_pulses_yield_kg_per_ha FLOAT,
    groundnut_area_1000_ha FLOAT,
    groundnut_production_1000_tons FLOAT,
    groundnut_yield_kg_per_ha FLOAT,
    sesamum_area_1000_ha FLOAT,
    sesamum_production_1000_tons FLOAT,
    sesamum_yield_kg_per_ha FLOAT,
    rapeseed_and_mustard_area_1000_ha FLOAT,
    rapeseed_and_mustard_production_1000_tons FLOAT,
    rapeseed_and_mustard_yield_kg_per_ha FLOAT,
    safflower_area_1000_ha FLOAT,
    safflower_production_1000_tons FLOAT,
    safflower_yield_kg_per_ha FLOAT,
    castor_area_1000_ha FLOAT,
    castor_production_1000_tons FLOAT,
    castor_yield_kg_per_ha FLOAT,
    linseed_area_1000_ha FLOAT,
    linseed_production_1000_tons FLOAT,
    linseed_yield_kg_per_ha FLOAT,
    sunflower_area_1000_ha FLOAT,
    sunflower_production_1000_tons FLOAT,
    sunflower_yield_kg_per_ha FLOAT,
    soyabean_area_1000_ha FLOAT,
    soyabean_production_1000_tons FLOAT,
    soyabean_yield_kg_per_ha FLOAT,
    oilseeds_area_1000_ha FLOAT,
    oilseeds_production_1000_tons FLOAT,
    oilseeds_yield_kg_per_ha FLOAT,
    sugarcane_area_1000_ha FLOAT,
    sugarcane_production_1000_tons FLOAT,
    sugarcane_yield_kg_per_ha FLOAT,
    cotton_area_1000_ha FLOAT,
    cotton_production_1000_tons FLOAT,
    cotton_yield_kg_per_ha FLOAT,
    fruits_area_1000_ha FLOAT,
    vegetables_area_1000_ha FLOAT,
    fruits_and_vegetables_area_1000_ha FLOAT,
    potatoes_area_1000_ha FLOAT,
    onion_area_1000_ha FLOAT,
    fodder_area_1000_ha FLOAT
);

copy agri_data from 'D:\MDTE21\Agri Data\cleaned_icrisat_data.csv' delimiter ',' csv header

select * from agri_data


--1. Year-wise Trend of Rice Production Across States (Top 3) 
WITH top_states AS (
    SELECT state_name
    FROM agri_data
    GROUP BY state_name
    ORDER BY SUM(rice_production_1000_tons) DESC
    LIMIT 3
)

SELECT year,state_name,SUM(rice_production_1000_tons) AS total_rice_production
FROM agri_data
WHERE state_name IN (SELECT state_name FROM top_states)
GROUP BY year, state_name
ORDER BY year, total_rice_production DESC;

--2. Top 5 Districts by Wheat Yield Increase Over the Last 5 Years
SELECT "dist_name", MAX("wheat_yield_kg_per_ha") - MIN("wheat_yield_kg_per_ha") AS yield_increase
FROM AGRI_DATA
WHERE "year" >= (SELECT MAX("year") FROM AGRI_DATA) - 5
GROUP BY "dist_name"
ORDER BY yield_increase DESC
LIMIT 5;

--3.States with the Highest Growth in Oilseed Production (5-Year Growth Rate)

SELECT state_name,ROUND((
(MAX(oilseeds_production_1000_tons) - MIN(oilseeds_production_1000_tons)) / NULLIF(MIN(oilseeds_production_1000_tons), 0))::NUMERIC, 3) AS growth_rate
FROM agri_data
WHERE year >= (SELECT MAX(year) FROM agri_data) - 5
GROUP BY state_name
ORDER BY growth_rate DESC
LIMIT 5;

--4.District-wise Correlation Between Area and Production for Major Crops (Rice, Wheat, and Maize) 

SELECT dist_name, 'Rice' AS crop, corr(rice_area_1000_ha, rice_production_1000_tons) AS correlation FROM agri_data GROUP BY dist_name
UNION ALL
SELECT dist_name, 'Wheat' AS crop, corr(wheat_area_1000_ha, wheat_production_1000_tons) FROM agri_data GROUP BY dist_name
UNION ALL
SELECT dist_name, 'Maize' AS crop, corr(maize_area_1000_ha, maize_production_1000_tons) FROM agri_data GROUP BY dist_name
ORDER BY dist_name, crop;

--5.Yearly Production Growth of Cotton in Top 5 Cotton Producing States

WITH top_states AS (SELECT state_name FROM agri_data GROUP BY state_name ORDER BY SUM(cotton_production_1000_tons) DESC LIMIT 5)
SELECT year, state_name, SUM(cotton_production_1000_tons) AS total_cotton
FROM agri_data
WHERE state_name IN (SELECT state_name FROM top_states)
GROUP BY year, state_name ORDER BY year, total_cotton DESC;

--6.Districts with the Highest Groundnut Production in 2020 

SELECT dist_name, state_name, SUM(groundnut_production_1000_tons) AS total_groundnut
FROM agri_data
WHERE year = 2020
GROUP BY dist_name, state_name
ORDER BY total_groundnut DESC
LIMIT 5;

--7.Annual Average Maize Yield Across All States 

SELECT year, ROUND(AVG(maize_yield_kg_per_ha)::NUMERIC, 2) AS avg_maize_yield
FROM agri_data
WHERE maize_yield_kg_per_ha IS NOT NULL
GROUP BY year
ORDER BY year;

--8.Total Area Cultivated for Oilseeds in Each State 

SELECT state_name, SUM(oilseeds_area_1000_ha) AS total_oilseed_area
FROM agri_data
WHERE oilseeds_area_1000_ha IS NOT NULL
GROUP BY state_name
ORDER BY total_oilseed_area DESC;

--9.Districts with the Highest Rice Yield

SELECT dist_name, state_name, ROUND(AVG(rice_yield_kg_per_ha)::NUMERIC, 2) AS avg_rice_yield
FROM agri_data
WHERE rice_yield_kg_per_ha IS NOT NULL
GROUP BY dist_name, state_name
ORDER BY avg_rice_yield DESC
LIMIT 5;

--10.Compare the Production of Wheat and Rice for the Top 5 States Over 10 Years 

WITH top_states AS (
SELECT state_name FROM agri_data GROUP BY state_name 
ORDER BY SUM(wheat_production_1000_tons + rice_production_1000_tons) DESC LIMIT 5)
SELECT year, state_name, SUM(wheat_production_1000_tons) AS wheat, SUM(rice_production_1000_tons) AS rice
FROM agri_data
WHERE state_name IN (SELECT state_name FROM top_states) AND year >= (SELECT MAX(year) FROM agri_data) - 9
GROUP BY year, state_name
ORDER BY year, state_name;

