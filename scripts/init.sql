/*
==========================================================
Creating Database and schemas
==========================================================
Purpose of Script:
	This script will create a new Database named datawarehouse. 
	It will check for the existance of this DB and if already exists it will drop and recreate the Database
	After creating Database, three schemas are created as per the layers of Medallion architecture - bronze, silver & gold.

!!!WARNING!!!
	Running this script will delete permanently the 'datawarehouse' and its data if the database already exists.
	Ensure to have backups before executing this script
*/

USE master;
GO

--Drop and recreate the Database - datawarehouse

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

--Create the 'datawarehouse' Database

CREATE DATABASE datawarehouse;
GO

USE datawarehouse;
GO

--Create Schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO


