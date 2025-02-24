pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',')
         AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
userNationality = FILTER pages by Nationality == 'Kazakhstan';
selectName = FOREACH userNationality GENERATE Name, Hobby;
STORE selectName INTO 'hdfs://localhost:9000/project2/TaskA.csv' USING PigStorage(',');