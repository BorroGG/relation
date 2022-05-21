table = LOAD 'hdfs:/user/di/relation/employee.csv'
USING PigStorage (' ') as (ServiceNumber, FirstName, LastName, MiddleName, Phone, Email, Fax, PositionID);

filter_table = FILTER table BY FirstName == 'Clarissa';

result = ORDER filter_table BY ServiceNumber;

STORE result INTO '/user/di/select_pig/select1';
