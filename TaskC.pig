pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',')
         AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
cleanedPages = FOREACH pages GENERATE PersonID, Nationality;
cleanPages = FILTER cleanedPages BY Nationality != 'Nationality';

citizensByCountry = GROUP cleanPages BY Nationality;
citizensByCountry = FOREACH citizensByCountry GENERATE group AS Nationality, COUNT(cleanPages) AS CitizensCount;

STORE citizensByCountry INTO 'hdfs://localhost:9000/project2/TaskC.csv' USING PigStorage(',');
