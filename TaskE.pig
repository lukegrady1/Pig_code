accessLogs = LOAD 'hdfs://localhost:9000/project2/access_logs.csv' USING PigStorage(',') AS (AccessID, ByWho, WhatPage, TypeOfAccess, AccessTime);
cleanedAccesslogs = FOREACH accessLogs GENERATE ByWho,WhatPage;
cleanAccesslogs = FILTER cleanedAccesslogs BY ByWho != 'ByWho';

accessLogsGroup = GROUP cleanAccesslogs BY ByWho;

finalOutput = FOREACH accessLogsGroup {
    distinctA = DISTINCT cleanAccesslogs.WhatPage;
    GENERATE
        group AS PersonID,
        COUNT(cleanAccesslogs) AS totalAccesses,
        COUNT(distinctA) AS distinctPages;
}
STORE finalOutput INTO 'hdfs://localhost:9000/project2/TaskE.csv' USING PigStorage(',');