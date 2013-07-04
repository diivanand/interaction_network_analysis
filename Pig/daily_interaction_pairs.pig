-- The purpose of this script is to generate tuples with the format 
-- (day [string], artifact creator [long], artifact responder [long], creation type [string], 
-- response type [string], # of responses in the given day [long])
-- characterizing the number and type of interaction pair events occurring
-- on a given day. 
--
-- Script arguments
-- $DBNAME          Name of the Jive instance. Used to construct a meaningful output directory name.
-- $DATADIR         Path to the directory of CSV files with the interaction pair event records 
--                  from a particular Jive instance.

REGISTER 'udfs.py' USING jython AS py;
REGISTER '/usr/lib/pig/contrib/piggybank/java/piggybank.jar';
REGISTER '/usr/lib/pig/contrib/piggybank/java/lib/joda-time-1.6.jar';
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE ISOToDay org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay();

%declare OUTPUTDIR1 '_daily_interaction_pairs'
%declare OUTPUTDIR $DBNAME$OUTPUTDIR1

rmf $OUTPUTDIR;

-- Load the interaction pair event data
pair_recs = LOAD '$DATADIR' USING PigStorage(',') AS 
                            (creator:long,responder:long, creationtype:chararray, 
                             responsetype:chararray, creationtime:long, responsetime:long,
                             locationtype:chararray,locationname:chararray);

--  Project record into the format used by the iPython iteraction analyzer. Notice the quantization to the day.
projection = FOREACH pair_recs GENERATE ISOToDay(UnixToISO(responsetime)) AS day,
                                  TOTUPLE(creator,responder) AS edge, creationtype, responsetype;
grpd = GROUP projection BY (day,edge,creationtype, responsetype);
counts = FOREACH grpd GENERATE group.day AS day, group.edge AS edge, group.creationtype AS creationtype, 
                             group.responsetype AS responsetype, COUNT(projection) AS cnts;
                             
-- Store the daily interaction pair event records
STORE counts INTO '$OUTPUTDIR' USING PigStorage(';');