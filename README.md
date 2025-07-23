# 🌾 AgriData Explorer: Indian Agricultural Insights

## 📘 Project Overview

**AgriData Explorer** is a comprehensive data analytics project designed to provide insights into India’s agricultural production trends using interactive visualizations in Power BI. The project uses a cleaned dataset from ICRISAT and connects to a PostgreSQL database using Python. This helps stakeholders like policy makers, analysts, and researchers track and compare crop performance across different states and districts over the last 50 years.

---

## 🎯 Objectives

- Analyze agricultural crop production data state-wise and district-wise
- Identify trends in rice, wheat, sugarcane, and oilseed crops
- Calculate crop yield efficiency and production growth
- Build an interactive Power BI dashboard with dropdown filters and visuals
- Use PostgreSQL for storing and querying structured agricultural data

---

## 🔧 Tools & Technologies Used

| Tool         | Purpose                                |
|--------------|-----------------------------------------|
| Python       | Data cleaning, preprocessing, and ETL   |
| PostgreSQL   | Backend relational database             |
| Power BI     | Visual analytics and dashboard design   |
| Pandas       | Data manipulation and transformation    |
| Matplotlib & Seaborn | Exploratory data analysis (EDA) |
| psycopg2-binary | PostgreSQL connector for Python      |

---

## 📊 Key Visualizations (Power BI)

1. **Year-wise Trend of Rice Production (Top 3 States)**  
   _Line chart with filter by top 3 rice-producing states over time_

2. **Districts with Highest Rice Yield**  
   _Bar chart with dropdown filter for selecting state_

3. **Wheat and Rice Production Comparison (Last 50 Years)**  
   _Line chart to observe production trends_

4. **Oilseed and Groundnut Production (Top States)**  
   _Bar and pie charts for state-wise share_

5. **Area vs. Production (Rice, Wheat, Maize)**  
   _Scatter plot to study relationship between cultivated area and yield_

6. **Map Chart**  
   _State-wise yield visualized geographically_

7. **Filter Controls (Slicers)**  
   _Year, State Name, Crop Type, Production Range_

---

