# SQL Data Warehouse Project

This project demonstrates the design and implementation of a full-scale SQL-based data warehouse using the layered architecture approach: **Bronze**, **Silver**, and **Gold** layers. It includes ETL processes, data cleaning, modeling, reporting views, and business analysis.

---

## 📁 Project Structure

The project is divided into several structured SQL scripts:

---

### 1. 🧱 Database and Schema Initialization
- **`creating_dataWareHouse_Project.sql`**  
  Creates the `DataWarehouse` database and defines the `bronze`, `silver`, and `gold` schemas.

---

### 2. 📥 Bronze Layer (Raw Data)
- **`bronze_layer_warehouse_project.sql`**  
  Creates the raw source tables under the `bronze` schema.
  
- **`Bulk_insert_warehouse_project.sql`**  
  Loads external CSV files into the bronze tables using `BULK INSERT`.

---

### 3. 🧹 Silver Layer (Cleaned & Transformed Data)
- **`silver_layer_warehouse.sql`**  
  Defines the structure of the silver layer tables.

- **`silver_inserting_clean_data.sql`**  
  Inserts transformed and cleaned data into the silver layer, applying business rules and data normalization.

---

<h3 style="color:red;">4. 🪙 Gold Layer (Business-Oriented Views)</h3>

- **`Gold_layer_warehouse.sql`**  
  Creates dimension (`dim_customers`, `dim_products`) and fact (`fact_sales`) views that serve analytical use cases.

---

<h3 style="color:red;">5. 📊 Business Reports</h3>

- **`Customer_Report.sql`**  
  Generates a report view of customer insights including segmentation, purchase value, and behavior over time.

- **`Product_Report.sql`**  
  Aggregates product-level performance metrics and classifies products into performance groups.

---

<h3 style="color:red;">6. 📈 Data Exploration & Business Analysis</h3>

- **`Explorartory_Data_Analysis.sql`**  
  Explores data distributions, dimensions, categories, and key time ranges.

- **`Data_Analysis.sql`**  
  Includes advanced analysis: trends over time, moving averages, customer/product segmentation, and revenue breakdowns.

- **`analyze_for_correction.sql`**  
  Contains validation queries and final adjustments used to refine the output and ensure data correctness.

---

## 🛠 Technologies Used
- Microsoft SQL Server  
---

## ✅ Key Concepts Demonstrated
- Layered Data Architecture (Bronze → Silver → Gold)  
- ETL with data validation, cleaning, and enrichment  
- Dimensional modeling and star schema design  
- Use of analytical SQL functions (`OVER()`, `LAG()`, `DATEDIFF()`...)  
- Business KPI computation and customer/product segmentation  
- Organized, modular SQL script design  

---

## 📎 Notes
- File paths in the `BULK INSERT` statements should be adapted to your local or server environment.  
- Views in the gold layer can be queried directly for use in BI tools or dashboards.


