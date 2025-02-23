friends = LOAD 'hdfs://localhost:9000/project2/friends.csv' USING PigStorage(',') AS (
    FriendRel: int,
    PersonID: int,
    MyFriend: int,
    DateofFriendship: int,
    Descr: chararray
);
cleanFriends = FILTER friends BY Descr != 'Desc';
cleanedFriends = FOREACH cleanFriends GENERATE PersonID,MyFriend;

groupFriends = GROUP cleanedFriends BY MyFriend;
countFriends = FOREACH groupFriends GENERATE group AS ID,COUNT(cleanedFriends) AS FriendCount;
avgData = GROUP countFriends ALL;
avgFriends = FOREACH avgData GENERATE AVG(countFriends.FriendCount) AS Average;
popular = FILTER countFriends BY FriendCount > avgFriends.Average;
report = FOREACH popular GENERATE ID;
STORE report INTO 'hdfs://localhost:9000/project2/TaskH.csv' USING PigStorage(',');