friends = LOAD 'hdfs://localhost:9000/project2/friends.csv' USING PigStorage(',') AS (FriendRel, PersonID, MyFriend, DateOfFriendship, Descr);
cleanFriends = FOREACH friends GENERATE PersonID,MyFriend;
cleanedFriends = FILTER cleanFriends BY PersonID != 'PersonID';

accessLogs = LOAD 'hdfs://localhost:9000/project2/access_logs.csv' USING PigStorage(',') AS (AccessID, ByWho, WhatPage, TypeOfAccess, AccessTime);
cleanAccessLogs = FOREACH accessLogs GENERATE ByWho,WhatPage;
cleanedAccessLogs = FILTER cleanAccessLogs BY ByWho != 'ByWho';

pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',') AS (ID, Name, Nationality, CountryCode, Hobby);
cleanPages = FOREACH pages GENERATE ID,Name;
cleanedPages = FILTER cleanPages BY Name != 'Name';

joinedOutput = JOIN cleanedFriends BY (PersonID,MyFriend) LEFT OUTER, cleanedAccessLogs BY (ByWho,WhatPage);
filteredOutput = FILTER joinedOutput BY access_logs_clean1::ByWho is null;
distinctPages = FOREACH (GROUP filteredOutput BY PersonID) GENERATE group AS PersonID;
joinedPages = JOIN distinctPages BY PersonID, cleanedPages BY ID;
output = FOREACH joinedPages GENERATE PersonID,Name;

STORE output INTO 'hdfs://localhost:9000/project2/TaskF.csv' USING PigStorage(',');