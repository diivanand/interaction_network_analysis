obj_id_dict = {33:'abuse',
            -786106556:'acclaimvote-like',
            22:'announcement',
            1400:'app',
            13:'attachment',
            2003:'audit message',
            26:'avatar',
            32:'ban',
            37:'blog',
            38:'blogpost',
            1000:'bridge',
            1001:'bridged content',
            601:'checkpoint',
            105:'comment',
            14:'community',
            109016030:'direct message',
            102:'document',
            129:'document backchannel',
            36:'draft',
            120:'document version',
            1150305777:'eaeacclaim-latest acclaim',
            801:'external url',
            800:'favorite',
            4:'group',
            3227383:'idea',
            1817418124:'inbox entry',
            111:'image',
            2:'message',
            18:'poll',
            20:'private message',
            501:'profile image',
            600:'project',
            604:'project status',
            27:'question',
            109:'search query',
            109400031:'share',
            700:'social group',
            701:'social group member',
            25:'status level',
            45:'status level scenario',
            1500:'stream channel',
            1501:'stream entry',
            17:'system',
            -2:'system container',
            41:'tag',
            42:'tag set',
            602:'task',
            1:'thread',
            40:'trackback',
            3:'user',
            2020:'user container',
            49:'user relationship',
            50:'user relationship graph',
            53:'user relationship list',
            48:'user status',
            1100:'video',
            1464927464:'wallentry-status update',
            550:'wiki link',
            -1:'null'
            }

# custom object types
obj_id_dict[96891546] = 'event'
obj_id_dict[535535779] = 'photo album'

@outputSchema("label:chararray")
def object_type_to_label(objtype):
    if objtype in obj_id_dict:
        return obj_id_dict[objtype]
    else:
        return str(objtype)

obj_label_dict = dict([[v,k] for k,v in obj_id_dict.items()])

act_id_dict = {0:'viewed',
            1:'created',
            2:'modified',
            3:'commented',
            4:'replied',
            5:'voted',
            6:'completed',
            7:'updatedStatus',
            8:'bookmarked',
            9:'rated',
            10:'blank',
            11:'liked',
            12:'joined',
            13:'connected',
            14:'followed',
            15:'unfollowed',
            16:'read',
            17:'shared',
            18:'NOTFOUND',
            19:'UNAUTHORIZED',
            20:'mentioned',         
            21:'promoted',
            22:'clicked',
            23:'logged_in',
            24:'logged_out',
            25:'applied',
            26:'removed',
            27:'repost',
            28:'object_exclusion_added',
            29:'object_exclusion_removed',
            30:'content_exclusion_added',
            31:'content_exclusion_removed',
            32:'user_deleted',
            33:'unread',
            34:'register_database',
            35:'manage',
            36:'unmanage',
            37:'tracked',
            38:'untracked',
            39:'allread',
            40:'inUserStream',
            41:'inUserInBox',
            42:'inUserActivityQueue',
            43:'unliked',
            44:'projectCompleted',
            45:'disinterest',
            46:'notification',
            47:'watch',
            48:'unwatch',
            49:'dismiss',
            50:'unconnected',
            51:'reshred_complete',
            52:'unjoined',
            53:'trace',
            54:'heartbeat',
            55:'moved',
            56:'repairFollowHint',
            57:'search',
            58:'user_search',
            59:'object_untrack_added',
            60:'object_untrack_removed',
            61:'digest'}

@outputSchema("label:chararray")
def activity_id_to_label(actid):
    if actid in act_id_dict:
        return act_id_dict[actid]
    else:
        return str(actid)
            
act_label_dict = dict([[v,k] for k,v in act_id_dict.items()])

@outputSchema("flag:int")
def is_relevant_record(field_val,values_to_accept):
    if type(values_to_accept) != tuple:
        values_to_accept = [values_to_accept]
    if field_val in values_to_accept:
        return 1
    else:
        return 0

@outputSchema("flag:int")
def is_relevant_activitytype(activitytype,values_to_accept):
    activitytype = activity_id_to_label(activitytype)
    return is_relevant_record(activitytype,values_to_accept)

@outputSchema("flag:int")
def is_relevant_descriptortype(descriptortype,values_to_accept):
    descriptortype = object_type_to_label(descriptortype)
    return is_relevant_record(descriptortype,values_to_accept)

@outputSchema("flag:int")
def is_relevant_acclaimtype(acclaimtype,value_to_accept):
	if acclaimtype == value_to_accept:
		return 1
	else:
		return 0

@outputSchema("cumsum:bag{}")
def cumsum(records,sort_idx,sum_idx):
    records.sort(key=lambda tup:tup[sort_idx])
    cslist = []
    s = 0
    for tup in records:
        s += tup[sum_idx]
        new_record = list(tup)
        new_record[sum_idx] = s
        cslist.append(tuple(new_record))
    return cslist    

from math import floor,ceil

@outputSchema("hist:bag{}")
def hist(user_activity,idx):
    h = {}
    for tup in user_activity:
        if tup[idx] in h:
            h[tup[idx]] += 1
        else:
            h[tup[idx]] = 1
    tups = h.items()
    tups.sort(key=lambda tup:tup[0])
    return tups
    
@outputSchema("percentiles:bag{}")
def user_percentiles(user_activity,levels):
    # Assuming user_activity contains tuples of the form (userid,count).
    activity_counts = [tup[1] for tup in user_activity]
    activity_counts.sort() 
    n = len(activity_counts)
    percentiles = []
    for level in levels:
        l = (n-1)*level
        i = int(floor(l))
        lmbda = l-i
        if i < n-1:
            percentiles.append(((1-lmbda)*activity_counts[i] + lmbda*activity_counts[i+1],level))
        else:
            percentiles.append((activity_counts[n-1],level))
    return percentiles
    
@outputSchema("tups:bag{}")
def filter_tups(tlist,inds_to_remove):
    new_tlist = []
    for tup in tlist:
        tup = list(tup)
        for ind in inds_to_remove:
            del tup[ind]
        tup = tuple(tup)
        new_tlist.append(tup)
    return new_tlist

@outputSchema("ms:long")
def quantize_epoch(ms,interval):
    """Given milliseconds from the epoch, returns milliseconds from the epoch quantized to the given interval."""
    if interval == 'day':
        return (ms/1000/3600/24)*1000*3600*24
    elif interval == 'hour':
        return (ms/1000/3600)*1000*3600
    elif interval == 'minute':
        return (ms/1000/60)*1000*60
    elif interval == 'second':
        return (ms/1000)*1000
