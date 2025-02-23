accessLogs = LOAD 'hdfs://localhost:9000/project2/access_logs.csv' USING PigStorage(',')
            AS (AccessId: int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime: chararray);
cleanAccesslogs = FOREACH accessLogs GENERATE ByWho, WhatPage;

userPages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',')
            AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

cleanUserPages = FOREACH userPages GENERATE PersonID, Name, Nationality;
accessLogsGrouped = GROUP cleanAccesslogs BY WhatPage;
accessLogsCount = FOREACH accessLogsGrouped GENERATE group AS PageID,COUNT(cleanAccesslogs) AS AccessCount;
sortedPages = ORDER accessLogsCount BY AccessCount DESC;
popularPages = LIMIT sortedPages 10;
mergedData = JOIN popularPages BY PageID, cleanUserPages BY PersonID;
output = FOREACH mergedData GENERATE PageID,Name,Nationality;
STORE output INTO 'hdfs://localhost:9000/project2/TaskB.csv' USING PigStorage(',');


