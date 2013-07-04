-- The purpose of this script is to generate a list of directed edges where the source is the responder to an artifact,
-- the target is the creator of the artifact and the weight is the number of times the responder has responded to the 
-- creator within the time window specified by $T1 and $T2.

-- Script arguments
-- $DBNAME    Name of the Jive instance to process. Loads a series of CSV files with the interaction pair event records 
--            from a particular Jive instance.
-- $T1        The beginning of the time window specified as milliseconds since the epoch. 
-- $T2        The end of the time window specified as milliseconds since the epoch. 
-- $THRES     Threshold on the event count.

REGISTER 'udfs.py' USING jython AS py;
REGISTER '/usr/lib/pig/contrib/piggybank/java/piggybank.jar';
REGISTER '/usr/lib/pig/contrib/piggybank/java/lib/joda-time-1.6.jar';

-- TODO: Change the time command line arguments from milliseconds to datetime strings
--DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

%declare INPUTDIR1 '_interaction_pairs'
%declare INPUTDIR '$DBNAME$INPUTDIR1' 

%declare OUTPUTDIR1 '_interaction_graph_'
%declare OUTPUTDIR2 '_'
%declare OUTPUTDIR $DBNAME$OUTPUTDIR1$T1$OUTPUTDIR2$T2

-- Default: no thresholding
%default THRES 1

rmf $OUTPUTDIR;

-- Load the interaction pair event data
pair_recs = LOAD '$INPUTDIR' USING PigStorage(',') AS 
                                (creator:long,responder:long,creationtype:chararray,responsetype:chararray,
                                 creationtime:long,responsetime:long,locationtype:chararray,
                                 locationname:chararray);

-- Get all pairs within the time window   
pair_recs_filtered = FILTER pair_recs BY (responsetime >= $T1 AND responsetime <= $T2);

-- Get the number of times responder responds to creator within the time period (directed interaction from B to A)
proj1 = FOREACH pair_recs_filtered GENERATE creator,responder,creationtype,responsetype,responsetime,locationtype,locationname;
grpd = GROUP proj1 BY (creator,responder);
counts = FOREACH grpd GENERATE group.responder AS responder, group.creator AS creator, COUNT(proj1) AS weight,
                             proj1.responsetype, proj1.creationtype, proj1.locationtype, proj1.locationname, 
                             proj1.responsetime;

-- Filter the edges by the counts as necessary
results = FILTER counts BY weight >= $THRES;

-- Store the data
STORE results INTO '$OUTPUTDIR' USING PigStorage(';');