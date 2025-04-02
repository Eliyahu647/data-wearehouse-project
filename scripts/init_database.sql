/*
    This script drops and recreates the 'DataWarehouse' database 
    and initializes its schema structure.

    It includes:
    - Dropping the database if it exists (with immediate rollback).
    - Creating a fresh instance of the database.
    - Creating the three standard schemas: bronze, silver, and gold.
*/


--Drop and recreate the data set 'DataWarehouse'
USE Master;
GO
  
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

--CREATE and USE Database DataWarehouse

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
