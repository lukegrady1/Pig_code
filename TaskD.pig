friends = LOAD 'hdfs://localhost:9000/project2/friends.csv' USING PigStorage(',') AS (FriendRel, PersonID, MyFriend, DateOfFriendship, Descr);
cleanFriends = FOREACH friends GENERATE FriendRel,PersonID,MyFriend;

pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',') AS (ID, Name, Nationality, CountryCode, Hobby);
cleanPages = FOREACH pages GENERATE ID,Name;
cleanedPages = FILTER cleanPages BY Name != 'Name';

groupFriends = GROUP cleanFriends BY MyFriend;
countFriends = FOREACH groupFriends GENERATE group,COUNT(cleanFriends) AS followers;
joinedOutput = join cleanedPages BY ID LEFT OUTER, countFriends BY group;
finalOutput = FOREACH joinedOutput GENERATE Name AS ownerName,(followers is not null ? followers : 0) AS count;
STORE finalOutput INTO 'hdfs://localhost:9000/project2/TaskD.csv' USING PigStorage(',');
