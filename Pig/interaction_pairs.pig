-- The purpose of this Pig script is to generate interaction pair event records of the form
-- (user A,user B,creation type,response type,creation time,response time,location type,location name)
--
-- Script arguments:
-- $DBNAME    Name of the Jive instance to process. Loads a series of CSV files with table
--            dumps from the specified instance.

REGISTER 'udfs.py' USING jython AS py;
REGISTER '/usr/lib/pig/contrib/piggybank/java/piggybank.jar';
REGISTER '/usr/lib/pig/contrib/piggybank/java/lib/joda-time-1.6.jar';
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE ISOToSecond org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToSecond();

-- Construct the names of the files to load and process
%declare DOCEXT '_jivedocument.csv'
%declare BPEXT  '_jiveblogpost.csv'
%declare USEXT  '_jiveuserstatus.csv'
%declare MESEXT '_jivemessage.csv' 
%declare COMEXT '_jivecomment.csv'
%declare ACCEXT '_jiveacclaim.csv'
%declare AVEXT  '_jiveacclaimvote.csv' 
%declare SGRPEXT '_jivesgroup.csv'
%declare COMMEXT '_jivecommunity.csv'
%declare CONTEXT '_jiveusercontainer.csv'
%declare BLOGEXT '_jiveblog.csv'
%declare PROJEXT '_jiveproject.csv'

-- Output directory extension
%declare OUTPUTDIREXT '_interaction_pairs'

-- Generate interaction pair event records for events where userA creates a given artifact and userB
-- comments on the given artifact.
define artifact_comment_interaction_pairs(artifact_records,artifact_id_ref,root_artifact_comment_records,creation_type_string)
returns pairs {

    -- Join the artifact records with the associated comment records tied to those artifacts
    artifact_comment_joined = JOIN $artifact_records BY $artifact_id_ref, $root_artifact_comment_records BY objectid;
    
    -- Project out the interaction pair event records
    artifact_comment_pairs = FOREACH artifact_comment_joined 
                             GENERATE $artifact_records::userid AS userA,
                                      $root_artifact_comment_records::userid AS userB,
                                      '$creation_type_string' AS creationtype, 'COM' AS responsetype,
                                      $artifact_records::creationdate AS creationtime,
                                      $root_artifact_comment_records::creationdate AS responsetime,
                                      $artifact_records::containertype AS containertype,
                                      $artifact_records::containerid AS containerid;
    
    -- Filter out any records displaying clear problems with time ordering
    $pairs = FILTER artifact_comment_pairs BY creationtime <= responsetime;    
    
};

rmf $DBNAME$OUTPUTDIREXT;

-- Load the jivedocument CSV dump
doc_records = LOAD '$DBNAME$DOCEXT' USING PigStorage(',') 
                AS (internaldocid:long, userid:long, documentid:chararray, 
                    typeid:long, creationdate:long, containertype:long, containerid:long);
                                    
-- Load the jiveblogpost CSV dump
blogpost_raw = LOAD '$DBNAME$BPEXT' USING PigStorage(',') 
                 AS (blogpostid:long, userid:long, creationdate:long, blogid:long);

-- Add containertype constant to all records
blogpost_records = FOREACH blogpost_raw GENERATE *, (long) 37 AS containertype;

-- Load the jiveuserstatus CSV dump
stat_records = LOAD '$DBNAME$USEXT' USING PigStorage(',') 
                 AS (userstatusid:long, userid:long, creationdate:long, containertype:long, 
                     containerid:long);

--Load the jivemessage CSV dump
msg_records = LOAD '$DBNAME$MESEXT' USING PigStorage(',') 
                AS (messageid:long, parentmessageid:long, threadid:long, userid:long, creationdate:long, 
                    containertype:long, containerid:long);
                    
-- Create duplicate for self join
msg_records_dup = FOREACH msg_records GENERATE *;

-- Load the jivecomment CSV dump
comment_records = LOAD '$DBNAME$COMEXT' USING PigStorage(',')
                    AS (commentid:long, parentcommentid:long, objecttype:long, objectid:long, userid:long, 
                        creationdate:long, parentobjecttype:long, parentobjectid:long);

-- Create duplicate for self join
comment_records_dup = FOREACH comment_records GENERATE *;
                                      
-- Load the jiveacclaim CSV dump
acclaim_records = LOAD '$DBNAME$ACCEXT' USING PigStorage(',') 
                    AS (acclaimid:long, descriptortype:long, descriptorid:long, acclaimtype:chararray,
                        creationdate:long, modificationdate:long);
                                            
-- Load the jiveacclaimvote CSV dump
vote_records = LOAD '$DBNAME$AVEXT' USING PigStorage(',') AS (voteid:long, voterid:long, acclaimid:long, creationdate:long,
                                                              modificationdate:long, votevalue:long);

-- Load the jivesgroup CSV dump
sgroup_records = LOAD '$DBNAME$SGRPEXT' USING PigStorage(',') AS (groupid:long, name:chararray);

-- Load the jivecommunity CSV dump
comm_records = LOAD '$DBNAME$COMMEXT' USING PigStorage(',') AS (communityid:long, name:chararray);

-- Load the jiveusercontainer CSV dump
cont_records = LOAD '$DBNAME$CONTEXT' USING PigStorage(',') AS (usercontainerid:long, userid:chararray);

-- Load the jiveblog CSV dump
blog_records = LOAD '$DBNAME$BLOGEXT' USING PigStorage(',') AS (blogid:long, name:chararray);

-- Load the jiveproject CSV dump
project_records = LOAD '$DBNAME$PROJEXT' USING PigStorage(',') AS (projectid:long, name:chararray);

-- Get root level response records to join with creation records
root_comment_records = FILTER comment_records BY parentcommentid IS NULL;

-- Filter out the root level comment records by artifact type and get root level message records
root_doc_comment_records = FILTER root_comment_records BY 
                            (py.is_relevant_descriptortype(objecttype,'document') == 1);
root_blogpost_comment_records = FILTER root_comment_records BY 
                            (py.is_relevant_descriptortype(objecttype,'blogpost') == 1);
root_status_comment_records = FILTER root_comment_records BY 
                            (py.is_relevant_descriptortype(objecttype,'wallentry-status update') == 1);

-- Get artifact-comment pairs
doc_comment_pairs = artifact_comment_interaction_pairs(doc_records,'internaldocid',root_doc_comment_records,'DOC');

blogpost_comment_joined = JOIN blogpost_records BY blogpostid, root_blogpost_comment_records BY objectid;
blogpost_comment_pairs = FOREACH blogpost_comment_joined GENERATE blogpost_records::userid AS userA,
                                                          root_blogpost_comment_records::userid AS userB,
                                                          'BLOGP' AS creationtype, 'COM' AS responsetype,
                                                          blogpost_records::creationdate AS creationtime,
                                                          root_blogpost_comment_records::creationdate AS responsetime,
                                                          blogpost_records::containertype AS containertype,
                                                          blogpost_records::blogid AS containerid;
blogpost_comment_pairs = FILTER blogpost_comment_pairs BY creationtime <= responsetime;
--blogpost_comment_pairs = artifact_comment_interaction_pairs(blogpost_records,'blogpostid',root_blogpost_comment_records,'BLOGP');

stat_comment_joined = JOIN stat_records BY userstatusid, root_status_comment_records BY objectid;
stat_comment_pairs = FOREACH stat_comment_joined GENERATE stat_records::userid AS userA,
                                               root_status_comment_records::userid AS userB,
                                               'STAT' AS creationtype, 'COM' AS responsetype,
                                               stat_records::creationdate AS creationtime,
                                               root_status_comment_records::creationdate AS responsetime,
                                               stat_records::containertype AS containertype,
                                               stat_records::containerid AS conatinerid;
stat_comment_pairs = FILTER stat_comment_pairs BY creationtime <= responsetime;
--stat_comment_pairs = artifact_comment_interaction_pairs(stat_records,'userstatusid',root_status_comment_records,'STAT');

-- Get comment-comment and message-message pairs
comment_comment_joined = JOIN comment_records BY commentid, comment_records_dup BY parentcommentid;
comment_comment_pairs = FOREACH comment_comment_joined GENERATE comment_records::userid AS userA,
                                                comment_records_dup::userid AS userB,
                                                'COM' AS creationtype, 'COM' AS responsetype,
                                                comment_records::creationdate AS creationtime,
                                                comment_records_dup::creationdate AS responsetime,
                                                comment_records::parentobjecttype AS containertype,
                                                comment_records::parentobjectid AS containerid;
comment_comment_pairs = FILTER comment_comment_pairs BY creationtime <= responsetime;

msg_msg_joined = JOIN msg_records BY messageid, msg_records_dup BY parentmessageid;
msg_msg_pairs = FOREACH msg_msg_joined GENERATE msg_records::userid AS userA,
                                                msg_records_dup::userid AS userB,
                                                'MSG' AS creationtype, 'MSG' AS responsetype,
                                                msg_records::creationdate AS creationtime,
                                                msg_records_dup::creationdate AS responsetime,
                                                msg_records::containertype AS containertype,
                                                msg_records::containerid AS containerid;
msg_msg_pairs = FILTER msg_msg_pairs BY creationtime <= responsetime;

-- Join the liker with their likes
like_joined = JOIN acclaim_records BY acclaimid, vote_records BY acclaimid;

-- Project
like_joined_proj = FOREACH like_joined GENERATE acclaim_records::acclaimtype AS acclaimtype, 
                                            acclaim_records::descriptortype AS descriptortype, 
                                            acclaim_records::descriptorid AS descriptorid, 
                                            vote_records::voterid AS userid, 
                                            vote_records::creationdate AS creationdate;
                                            
-- Get likes by artifact
like_doc_records = FILTER like_joined_proj BY acclaimtype=='like' AND 
                                    (py.is_relevant_descriptortype(descriptortype,'document') == 1);

like_blogpost_records = FILTER like_joined_proj BY acclaimtype=='like' AND 
                                    (py.is_relevant_descriptortype(descriptortype,'blogpost') == 1);

like_stat_records = FILTER like_joined_proj BY acclaimtype=='like' AND 
                                    (py.is_relevant_descriptortype(descriptortype,'wallentry-status update') == 1);

like_msg_records = FILTER like_joined_proj BY acclaimtype=='like' AND 
                                    (py.is_relevant_descriptortype(descriptortype,'message') == 1);

like_comment_records = FILTER like_joined_proj BY acclaimtype=='like' AND 
                                    (py.is_relevant_descriptortype(descriptortype,'comment') == 1);

--Get artifact-like pairs
doc_like_joined = JOIN doc_records BY internaldocid, like_doc_records BY descriptorid;
doc_like_pairs = FOREACH doc_like_joined GENERATE doc_records::userid AS userA, like_doc_records::userid AS userB,
                                                'DOC' AS creationtype, 'LIKE' AS responsetype,
                                                doc_records::creationdate AS creationtime,
                                                like_doc_records::creationdate AS responsetime,
                                                doc_records::containertype AS containertype,
                                                doc_records::containerid AS containerid;
doc_like_pairs = FILTER doc_like_pairs BY creationtime <= responsetime;

blogpost_like_joined = JOIN blogpost_records BY blogpostid, like_blogpost_records BY descriptorid;
blogpost_like_pairs = FOREACH blogpost_like_joined GENERATE blogpost_records::userid AS userA, like_blogpost_records::userid AS userB,
                                                'BLOGP' AS creationtype, 'LIKE' AS responsetype,
                                                blogpost_records::creationdate AS creationtime,
                                                like_blogpost_records::creationdate AS responsetime,
                                                blogpost_records::containertype AS containertype,
                                                blogpost_records::blogid AS containerid;
blogpost_like_pairs = FILTER blogpost_like_pairs BY creationtime <= responsetime;

stat_like_joined = JOIN stat_records BY userstatusid, like_stat_records BY descriptorid;
stat_like_pairs = FOREACH stat_like_joined GENERATE stat_records::userid AS userA, like_stat_records::userid AS userB,
                                                'STAT' AS creationtype, 'LIKE' AS responsetype,
                                                stat_records::creationdate AS creationtime,
                                                like_stat_records::creationdate AS responsetime,
                                                stat_records::containertype AS containertype,
                                                stat_records::containerid AS containerid;
stat_like_pairs = FILTER stat_like_pairs BY creationtime <= responsetime;

comment_like_joined = JOIN comment_records BY commentid, like_comment_records BY descriptorid;
comment_like_pairs = FOREACH comment_like_joined GENERATE comment_records::userid AS userA, like_comment_records::userid AS userB,
                                                'COM' AS creationtype, 'LIKE' AS responsetype,
                                                comment_records::creationdate AS creationtime,
                                                like_comment_records::creationdate AS responsetime,
                                                comment_records::parentobjecttype AS containertype,
                                                comment_records::parentobjectid AS containerid;
comment_like_pairs = FILTER comment_like_pairs BY creationtime <= responsetime;

msg_like_joined = JOIN msg_records BY messageid, like_msg_records BY descriptorid;
msg_like_pairs = FOREACH msg_like_joined GENERATE msg_records::userid AS userA, like_msg_records::userid AS userB,
                                                'MSG' AS creationtype, 'LIKE' AS responsetype,
                                                msg_records::creationdate AS creationtime,
                                                like_msg_records::creationdate AS responsetime,
                                                msg_records::containertype AS containertype,
                                                msg_records::containerid AS containerid;
msg_like_pairs = FILTER msg_like_pairs BY creationtime <= responsetime;

-- Union all of the event records together
un1 = UNION doc_comment_pairs, blogpost_comment_pairs;
un2 = UNION un1, stat_comment_pairs;
un3 = UNION un2, comment_comment_pairs;
un4 = UNION un3, msg_msg_pairs;
un5 = UNION un4, doc_like_pairs;
un6 = UNION un5, blogpost_like_pairs;
un7 = UNION un6, stat_like_pairs;
un8 = UNION un7, comment_like_pairs;
all_event_records = UNION un8, msg_like_pairs;

-- Attach location names

-- Parition event records by location type
records_loc_sgroup = FILTER all_event_records BY (py.is_relevant_descriptortype(containertype,'social group') == 1);
records_loc_blog = FILTER all_event_records BY (py.is_relevant_descriptortype(containertype,'blog') == 1);
records_loc_community = FILTER all_event_records BY (py.is_relevant_descriptortype(containertype,'community') == 1);
records_loc_container = FILTER all_event_records BY (py.is_relevant_descriptortype(containertype,'user container') == 1);
records_loc_project = FILTER all_event_records BY (py.is_relevant_descriptortype(containertype,'project') == 1);

-- There may be more than one representaion of NULL as we've seen in other tables which is why I'm doing this instead.
-- I need to guarantee that all these subsets form a partition of the entire set of all event records, and this ensures that.
records_noloc = FILTER all_event_records BY (py.is_relevant_descriptortype(containertype,'social group') != 1) AND
                              (py.is_relevant_descriptortype(containertype,'project') != 1) AND
                              (py.is_relevant_descriptortype(containertype,'blog') != 1) AND
                              (py.is_relevant_descriptortype(containertype,'user container') != 1) AND
                              (py.is_relevant_descriptortype(containertype,'community') != 1);

-- Join records with container tables to obtain container names
sgroup_joined = JOIN records_loc_sgroup BY containerid, sgroup_records BY groupid;
ext_sgroup_records = FOREACH sgroup_joined GENERATE records_loc_sgroup::userA AS userA, records_loc_sgroup::userB AS userB, 
                                             records_loc_sgroup::creationtype AS creationtype, 
                                             records_loc_sgroup::responsetype AS responsetype,
                                             records_loc_sgroup::creationtime AS creationtime,
                                             records_loc_sgroup::responsetime AS responsetime,
                                             'SGROUP' AS containertype,
                                             sgroup_records::name AS containername;
                                             
blog_joined = JOIN records_loc_blog BY containerid, blog_records BY blogid;
ext_blog_records = FOREACH blog_joined GENERATE records_loc_blog::userA AS userA, records_loc_blog::userB AS userB, 
                                             records_loc_blog::creationtype AS creationtype, 
                                             records_loc_blog::responsetype AS responsetype,
                                             records_loc_blog::creationtime AS creationtime,
                                             records_loc_blog::responsetime AS responsetime,
                                             'BLOG' AS containertype,
                                             blog_records::name AS containername;                                             
                                             
comm_joined = JOIN records_loc_community BY containerid, comm_records BY communityid;
ext_comm_records = FOREACH comm_joined GENERATE records_loc_community::userA AS userA, records_loc_community::userB AS userB, 
                                             records_loc_community::creationtype AS creationtype, 
                                             records_loc_community::responsetype AS responsetype,
                                             records_loc_community::creationtime AS creationtime,
                                             records_loc_community::responsetime AS responsetime,
                                             'COMMU' AS containertype,
                                             comm_records::name AS containername;

cont_joined = JOIN records_loc_container BY containerid, cont_records BY usercontainerid;
ext_cont_records = FOREACH cont_joined GENERATE records_loc_container::userA AS userA, records_loc_container::userB AS userB, 
                                             records_loc_container::creationtype AS creationtype, 
                                             records_loc_container::responsetype AS responsetype,
                                             records_loc_container::creationtime AS creationtime,
                                             records_loc_container::responsetime AS responsetime,
                                             'CONT' AS containertype,
                                             cont_records::userid AS containername;
                                             
proj_joined = JOIN records_loc_project BY containerid, project_records BY projectid;
ext_project_records = FOREACH proj_joined GENERATE records_loc_project::userA AS userA, records_loc_project::userB AS userB, 
                                             records_loc_project::creationtype AS creationtype, 
                                             records_loc_project::responsetype AS responsetype,
                                             records_loc_project::creationtime AS creationtime,
                                             records_loc_project::responsetime AS responsetime,
                                             'PROJECT' AS containertype,
                                             project_records::name AS containername;
                                             
ext_noloc_records = FOREACH records_noloc GENERATE userA, userB, creationtype, responsetype, creationtime, responsetime,
                                               'NOLOC' AS containertype, 'NONAME' AS containername;
                                             
-- Union them together
ev1 = UNION ext_sgroup_records, ext_blog_records;
ev2 = UNION ext_comm_records, ext_cont_records;
ev3 = UNION ext_project_records, ext_noloc_records;
ev4 = UNION ev1, ev2;
all_extended_event_records = UNION ev3, ev4;              
                                                                                        
-- Remove any self-loops
events_to_save = FILTER all_extended_event_records BY userA != userB;

-- Store the results
STORE events_to_save INTO '$DBNAME$OUTPUTDIREXT' USING PigStorage(',');