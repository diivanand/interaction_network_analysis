#!/usr/bin/env python
from optparse import OptionParser
from jive_analytics.db_access.PostgresDB import PostgresDB

def write_rows_to_csv_file(rows,db,tbl):
    f = open('%s_%s.csv' % (db,tbl),'w')
    for i in range(1,len(rows)):
        row = []
        for el in rows[i]:
            if type(el) == unicode:
                row.append('\"'+el.encode('ascii','ignore')+'\"')
            elif el == None:
                row.append('\"\"')
            else:
                row.append(str(el))
        s = ','.join(row)+'\n'
        f.write(s)
    f.close()

def select_fields_to_include(field_names):
    selected = []
    field_names.sort()
    while True:
        i = 0
        if len(field_names) == 0:
            return selected
        for field in field_names:
            print "%d. %s" % (i+1,field)
            i += 1
        print "0. Done."
        print "Selected: %s" % ','.join(selected)
        d = input("Selection: ")
        if d == 0:
            return selected
        elif d > 0 and d <= len(field_names):
            selected.append(field_names[d-1])
            field_names.remove(field_names[d-1])
          
def main():
    
    # Define the commandline option parser
    parser = OptionParser()
    parser.add_option('-d','--db',action='store',type='string',dest='db',help='Local database to access')
    parser.add_option("-u","--user",action='store',type='string',dest='user',help='Database username')
    
    # Parse the commandline options
    (options, args) = parser.parse_args()
    
    # Check to make sure the user and database were specified
    if options.db == None:
        print "The database was not specified! Exiting."
        return
    elif options.user == None:
        print "The username was not specified! Exiting."
        return
    
    # Connect to the database
    db = PostgresDB(db=options.db,user=options.user)
    
    # Select the distinct user profile field names from the jiveprofilefield table
    print "Fetching the distinct user profiles field names from the jiveprofilefield table."
    results = db.execute('select name,fieldid from jiveprofilefield')
    field_dict = dict(results)
    field_names = field_dict.keys()
    selected_field_names = select_fields_to_include(field_names)
    selected_field_ids = [int(field_dict[name]) for name in selected_field_names]
    
    # Dump the restricted user profile field data 
    print "Writing out the restricted user profile data."
    query = 'select t2.userid as userid, t1.name as name, t2.value as value \
             from jiveprofilefield as t1, jiveuserprofile as t2 where t1.fieldid = t2.fieldid and t1.fieldid in '
    query += str(tuple(selected_field_ids))
    rows = db.execute(query)
    write_rows_to_csv_file(rows,options.db,'restricted_userprofile_info')
    
    # Dump relevant columns from the jivedocument table
    print "Writing out the relevant columns from the jivedocument table."
    rows = db.fetch_table(tbl='jivedocument',columns=['internaldocid','userid','documentid','typeid','creationdate', 
                                                      'containertype','containerid'])
    write_rows_to_csv_file(rows,options.db,'jivedocument')
    
    # Dump relevant columns from the jiveblogpost table
    print "Writing out the relevant columns from the jiveblogpost table."
    rows = db.fetch_table(tbl='jiveblogpost',columns=['blogpostid','userid','creationdate','blogid'])
    write_rows_to_csv_file(rows,options.db,'jiveblogpost')
    
    # Dump relevant columns from the jiveuserstatus table
    print "Writing out the relevant columns from the jiveuserstatus table."
    rows = db.fetch_table(tbl='jiveuserstatus',columns=['userstatusid','userid','creationdate','containertype', 
                                                        'containerid'])
    write_rows_to_csv_file(rows,options.db,'jiveuserstatus')

    # Dump relevant columns from the jivemessage table
    print "Writing out the relevant columns from the jivemessage table."
    rows = db.fetch_table(tbl='jivemessage',columns=['messageid', 'parentmessageid', 'threadid', 'userid', 
                                                     'creationdate', 'containertype', 'containerid'])
    write_rows_to_csv_file(rows,options.db,'jivemessage')

    # Dump relevant columns from the jivecomment table
    print "Writing out the relevant columns from the jivecomment table."
    rows = db.fetch_table(tbl='jivecomment',columns=['commentid', 'parentcommentid', 'objecttype', 'objectid', 
                                                     'userid', 'creationdate', 'parentobjecttype', 'parentobjectid'])
    write_rows_to_csv_file(rows,options.db,'jivecomment')

    # Dump relevant columns from the jiveacclaim table
    print "Writing out the relevant columns from the jiveacclaim table."
    rows = db.fetch_table(tbl='jiveacclaim',columns=['acclaimid','objecttype', 'objectid', 'acclaimtype', 
                                                     'creationdate', 'modificationdate'])
    write_rows_to_csv_file(rows,options.db,'jiveacclaim')
    
    # Dump relevant columns from the jiveacclaimvote table
    print "Writing out the relevant columns from the jiveacclaimvote table."
    rows = db.fetch_table(tbl='jiveacclaimvote',columns=['voteid','voterid','acclaimid','creationdate',
                          'modificationdate','votevalue'])
    write_rows_to_csv_file(rows,options.db,'jiveacclaimvote')

    # Dump relevant columns from the jivesgroup table
    print "Writing out the relevant columns from the jivesgroup table."
    rows = db.fetch_table(tbl='jivesgroup',columns=['groupid','name'])
    write_rows_to_csv_file(rows,options.db,'jivesgroup')

    # Dump relevant columns from the jivecommunity table
    print "Writing out the relevant columns from the jivecommunity table."
    rows = db.fetch_table(tbl='jivecommunity',columns=['communityid','name'])
    write_rows_to_csv_file(rows,options.db,'jivecommunity')

    # Dump relevant columns from the jiveusercontainer table
    print "Writing out the relevant columns from the jiveusercontainer table."
    rows = db.fetch_table(tbl='jiveusercontainer',columns=['usercontainerid','userid'])
    write_rows_to_csv_file(rows,options.db,'jiveusercontainer')

    # Dump relevant columns from the jiveblog table
    print "Writing out the relevant columns from the jiveblog table."
    rows = db.fetch_table(tbl='jiveblog',columns=['blogid','name'])
    write_rows_to_csv_file(rows,options.db,'jiveblog')
    
    # Dump relevant columns from the jiveproject table
    print "Writing out the relevant columns from the jiveproject table."
    rows = db.fetch_table(tbl='jiveproject',columns=['projectid','name'])
    write_rows_to_csv_file(rows,options.db,'jiveproject')

if __name__ == '__main__':
    main()
        
        