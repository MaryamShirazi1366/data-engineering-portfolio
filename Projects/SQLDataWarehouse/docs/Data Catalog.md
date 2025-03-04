# Data Catalog for Gold Layer

## Overview
The **Gold Layer** serves as the business-level representation of data, structured for analytical and reporting purposes. It consists of **dimension tables** and **fact tables**, which store business-critical metrics and contextual details for enhanced insights.

---

## **Dimension Tables**

### 1. **gold.dim_customers**
- **Purpose:** Contains enriched customer data, including demographic and geographic details, to support customer-centric analysis.
- **Columns:**

| Column Name      | Data Type     | Description                                                                                   |
|------------------|---------------|-----------------------------------------------------------------------------------------------|
| customer_key     | INT           | A surrogate key uniquely identifying each customer record in this table.                      |
| customer_id      | INT           | The unique identifier assigned to each customer in the source system.                        |
| customer_number  | NVARCHAR(50)  | Alphanumeric identifier used for tracking and referencing customers.                         |
| first_name       | NVARCHAR(50)  | Customer's given name.                                                                       |
| last_name        | NVARCHAR(50)  | Customer's surname or family name.                                                          |
| country          | NVARCHAR(50)  | Country of residence (e.g., 'Australia').                                                   |
| marital_status   | NVARCHAR(50)  | Marital status of the customer (e.g., 'Married', 'Single').                                 |
| gender           | NVARCHAR(50)  | Gender classification (e.g., 'Male', 'Female', 'n/a').                                      |
| birthdate        | DATE          | Date of birth formatted as YYYY-MM-DD (e.g., 1971-10-06).                                   |
| create_date      | DATE          | Timestamp indicating when the customer record was created in the system.                     |

---

### 2. **gold.dim_products**
- **Purpose:** Provides metadata about products, including categorization, pricing, and maintenance requirements.
- **Columns:**

| Column Name         | Data Type     | Description                                                                                   |
|---------------------|---------------|-----------------------------------------------------------------------------------------------|
| product_key         | INT           | Surrogate key uniquely identifying each product.                                              |
| product_id          | INT           | Unique identifier assigned to the product in the source system.                              |
| product_number      | NVARCHAR(50)  | Structured alphanumeric code representing the product for inventory and categorization.       |
| product_name        | NVARCHAR(50)  | Descriptive name, often including type, color, and size.                                     |
| category_id         | NVARCHAR(50)  | Identifier linking the product to a broader category.                                        |
| category            | NVARCHAR(50)  | High-level classification (e.g., 'Bikes', 'Components').                                    |
| subcategory         | NVARCHAR(50)  | More detailed classification within the category.                                           |
| maintenance_required| NVARCHAR(50)  | Indicates if the product requires maintenance ('Yes'/'No').                                 |
| cost                | INT           | Base price of the product in whole currency units.                                           |
| product_line        | NVARCHAR(50)  | Product series or specialization (e.g., 'Road', 'Mountain').                               |
| start_date          | DATE          | Availability date of the product in the market.                                             |

---

## **Fact Tables**

### 3. **gold.fact_sales**
- **Purpose:** Stores transactional data related to sales, linking customers and products for sales analytics.
- **Columns:**

| Column Name     | Data Type     | Description                                                                                   |
|-----------------|---------------|-----------------------------------------------------------------------------------------------|
| order_number    | NVARCHAR(50)  | Unique alphanumeric identifier for each sales order (e.g., 'SO54496').                        |
| product_key     | INT           | Foreign key linking to the product dimension table.                                           |
| customer_key    | INT           | Foreign key linking to the customer dimension table.                                          |
| order_date      | DATE          | The date when the order was placed.                                                           |
| shipping_date   | DATE          | The date the order was shipped.                                                               |
| due_date        | DATE          | The payment due date for the order.                                                          |
| sales_amount    | INT           | Total value of the sale in whole currency units (e.g., 25).                                   |
| quantity        | INT           | Number of units ordered for the specific product.                                            |
| price           | INT           | Price per unit of the product in whole currency units.                                       |

---

## Summary
This Gold Layer schema is designed to provide structured, high-quality business data for analytics and reporting. The **dimension tables** offer detailed context on customers and products, while the **fact tables** capture transactional sales data, enabling in-depth performance analysis and decision-making.

