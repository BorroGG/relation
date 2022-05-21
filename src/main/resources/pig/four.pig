table1 = LOAD 'hdfs:/user/di/relation/position.csv'
USING PigStorage (' ') as (PositionID, Title);

table2 = LOAD 'hdfs:/user/di/relation/employee.csv'
USING PigStorage (' ') as (ServiceNumber, FirstName, LastName, MiddleName, Phone, Email, Fax, PositionID);

join_table = JOIN table1 BY PositionID, table2 BY PositionID;

STORE join_table INTO '/user/di/select_pig/select4';
