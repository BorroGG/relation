table = LOAD 'hdfs:/user/di/relation/mission.csv'
USING PigStorage (' ') as (MissionID, Name, Begin_Date, End_Date, Author, Executor);

filter_table = FILTER table BY Executor > 4;
result = ORDER filter_table BY MissionID;

STORE result INTO '/user/di/select_pig/select2';
