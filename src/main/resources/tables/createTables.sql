CREATE TABLE Position
(
    PositionID SMALLINT NOT NULL PRIMARY KEY,
    Title VARCHAR(20) NOT NULL
);

CREATE TABLE Employee
(
    ServiceNumber INTEGER NOT NULL PRIMARY KEY,
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    MiddleName VARCHAR(50),
    Phone CHAR(11) NOT NULL,
    Email VARCHAR(40) NOT NULL,
    Fax CHAR(5) NOT NULL,
    PositionID SMALLINT REFERENCES Position (PositionID)
);


CREATE TABLE Mission
(
    MissionID INTEGER NOT NULL PRIMARY KEY,
    Name VARCHAR(30) NOT NULL,
    Begin_Date DATE NOT NULL,
    End_Date DATE,
    Author SMALLINT REFERENCES Employee (ServiceNumber),
    Executor SMALLINT REFERENCES Employee (ServiceNumber)
);
