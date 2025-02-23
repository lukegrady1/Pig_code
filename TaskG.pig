accessLogs = LOAD 'hdfs://localhost:9000/project2/access_logs.csv' USING PigStorage(',') AS (
    AccessId: int,
    ByWho: int,
    WhatPage: int,
    TypeOfAccess: chararray,
    AccessTime: chararray
);
accessLogsFilter = FILTER accessLogs BY TypeOfAccess != 'TypeOfAccess';
accessLogsFiltered = FOREACH accessLogsFilter GENERATE ByWho AS PersonID,AccessTime;

pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',') AS (
    ID: int,
    Name: chararray,
    Nationality: chararray,
    CountryCode: int,
    Hobby: chararray
);
pagesFilter = FILTER pages BY Name != 'Name';
pagesFiltered = FOREACH pagesFilter GENERATE ID AS ID,Name;
groupLogs = GROUP accessLogsFiltered BY PersonID;
accessTime = FOREACH groupLogs GENERATE group AS PersonID,ToDate(MAX(accessLogsFiltered.AccessTime),'yyyy-MM-dd HH:mm:ss') AS LatestAccessTime:datetime;
peopleDisconnected = FILTER accessTime BY ABS(DaysBetween(LatestAccessTime,CurrentTime())) > (long)14;
peopleDisconnectedFiltered = FOREACH peopleDisconnected GENERATE PersonID AS PersonID;
people = JOIN peopleDisconnectedFiltered BY PersonID, pagesFiltered BY ID;
output = FOREACH people GENERATE disconnected_people_filter::PersonID, filtered_pages1::Name;
STORE output INTO 'hdfs://localhost:9000/project2/TaskG.csv' USING PigStorage(',');

