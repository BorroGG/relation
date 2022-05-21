table1 = LOAD 'hdfs:/user/di/relation/mission.csv'
USING PigStorage (' ') as (MissionID, Name, Begin_Date, End_Date, Author, Executor);

table2 = LOAD 'hdfs:/user/di/relation/employee.csv'
USING PigStorage (' ') as (ServiceNumber, FirstName, LastName, MiddleName, Phone, Email, Fax, PositionID);

join_table = JOIN table1 BY Executor, table2 BY ServiceNumber;

filter_table = FILTER join_table BY (FirstName matches '.*[a].*');

STORE filter_table INTO '/user/di/select_pig/select5';

