/*
=============================================================
Database and Schema Setup Script
=============================================================

Purpose:
    This script creates a new database named 'DataWarehouse' and sets up three schemas: 
    'bronze', 'silver', and 'gold'. If the database already exists, it is dropped and recreated.

Important Notes:
    - This script will **permanently delete** the existing 'DataWarehouse' database if it exists.
    - All data within the database will be **lost**. 
    - Ensure proper backups are taken before executing this script.

Schemas Overview:
    - **Bronze**: Stores raw, unprocessed data from various sources.
    - **Silver**: Contains cleaned and standardized data for further transformation.
    - **Gold**: Houses business-ready data optimized for reporting and analytics.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
