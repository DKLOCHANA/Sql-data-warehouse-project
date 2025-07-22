-- ================================================
-- Script No.01
-- Create Datawarehouse DB
-- Creates bronze, silver, and gold schemas
-- ================================================

USE master;
GO

-- Drop the database if it already exists (use with caution!)
IF DB_ID('Datawarehouse') IS NOT NULL
BEGIN
    ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Datawarehouse;
END
GO

-- Create the Datawarehouse database
CREATE DATABASE Datawarehouse;
GO

-- Switch to the newly created database
USE Datawarehouse;
GO

-- Create bronze schema
CREATE SCHEMA bronze;
GO

-- Create silver schema
CREATE SCHEMA silver;
GO

-- Create gold schema
CREATE SCHEMA gold;
GO
