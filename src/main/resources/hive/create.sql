DROP DATABASE IF EXISTS office CASCADE;
CREATE DATABASE office;
USE office;

CREATE TABLE Position (
    PositionID INT,
    Title VARCHAR(30))
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ' ';
LOAD DATA INPATH '/user/di/relation/position.csv'
    INTO TABLE Position;

CREATE TABLE Employee (
    ServiceNumber INT,
    FirstName VARCHAR(30),
    LastName VARCHAR(50),
    MiddleName VARCHAR(50),
    Phone VARCHAR(30),
    Email VARCHAR(30),
	Fax VARCHAR(30),
	PositionID INT)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ' ';
LOAD DATA INPATH '/user/di/relation/employee.csv'
    INTO TABLE Employee;

CREATE TABLE Mission (
    MissionID INT,
    Name VARCHAR(30),
    Begin_Date VARCHAR(50),
    End_Date VARCHAR(50),
    Author INT,
    Executor INT)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ' ';
LOAD DATA INPATH '/user/di/relation/mission.csv'
    INTO TABLE Mission;
