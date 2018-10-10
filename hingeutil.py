import os

class Util:
    def __init__ (self):
        pass
    @staticmethod
    def FileProcessingCmds (tblName, bucket, tgtKey, rs_s3_role_arn):
        cmdList = ["create table staging."+ tblName + "(like staging.tbl_rating_file);"] # create table same as file name
        #clean load tables this next file processing
        cmdList.append("truncate table staging.l_dim_user;")
        cmdList.append("truncate table staging.l_connection;")
        cmdList.append("truncate table staging.l_connection_missing;")
        cmdList.append("truncate table staging.l_connection_raw;")
        cmdList.append("truncate table staging.l_fct_ratings;")
        cmdList.append("copy staging."+ tblName + " from 's3://"+bucket+os.sep+tgtKey+"' iam_role '"+ rs_s3_role_arn +"' delimiter '\t';") # load data into above table
        cmdList.append( "insert into staging.l_dim_user(user_name) (select user_from from staging."+ tblName + " union select user_from from staging."+ tblName
            + ") except select user_name from dw.dim_user;")
        cmdList.append( "insert into staging.l_connection_raw(user_from,user_to) select user_from, user_to from staging."+ tblName
        + " except select user_from, user_to from dw.dim_connection;")
        query = '''
        insert into staging.l_connection_missing
            select min(connection_id),user_to,user_from
            from staging.l_connection_raw a where not exists (select 1 from staging.l_connection_raw b where b.user_from = a.user_to and b.user_to = a.user_from)
            group by user_to, user_from;
        '''
        cmdList.append(query)
        query = '''
            insert into staging.l_connection
            select min(connection_id) min_conn, user_from, user_to
            from (
                select connection_id, user_from, user_to
                from staging.l_connection_raw
                union
                select b.connection_id, a.user_from, a.user_to
                from staging.l_connection_raw a
                join staging.l_connection_raw b on a.user_from = b.user_to and a.user_to = b.user_from
                and a.connection_id > b.connection_id)
            group by user_from,user_to
            union
            select connection_id, user_from, user_to from staging.l_connection_missing;
        '''
        cmdList.append(query)
        cmdList.append("insert into dw.dim_user (user_name) select user_name from staging.l_dim_user ;")
        cmdList.append("insert into dw.dim_connection (connection_id,user_from,user_to) select connection_id,user_from,user_to from staging.l_connection;")

        query = '''
            insert into staging.l_fct_ratings (connection_id, rating_ts, rating_type, user_from)
            select c.connection_id, f.rating_ts, f.rating_type, f.user_from
            from staging.''' + tblName + ' f join dw.dim_connection c on c.user_from = f.user_from and c.user_to = f.user_to;'
        cmdList.append(query)
        # if some of the ratings are restatement then let's remove them
        cmdList.append('''delete from    dw.fct_ratings
            using  staging.l_fct_ratings
                where dw.fct_ratings.connection_id= staging.l_fct_ratings.connection_id
                  and dw.fct_ratings.rating_ts = staging.l_fct_ratings.rating_ts;''')
        cmdList.append('''insert into dw.fct_ratings (connection_id, rating_ts, rating_type, user_from)
        select connection_id, rating_ts, rating_type, user_from
          from staging.l_fct_ratings;''')
        # need to refresh accumulating snapshot for only connections received in this file
        cmdList.append('''delete from dw.fct_rating_accumulate
            using  staging.l_fct_ratings where dw.fct_rating_accumulate.connection_id= staging.l_fct_ratings.connection_id;''')
        cmdList.append('''insert  into dw.fct_rating_accumulate
        (connection_id, skip_cnt, like_ts, like_wo_comment_ts, like_w_comment_ts, match_ts, non_report_terminal_ts, report_ts,terminal_ts)
        with need_update as (select distinct connection_id  from staging.l_fct_ratings)
        select
            f.connection_id,
            sum(case when rating_type = 0 then 1 else 0 end) skip_cnt,
            min(case when rating_type in (1,2) then rating_ts else null end) like_ts,
            min(case when rating_type =1 then rating_ts else null end) like_wo_comment_ts,
            min(case when rating_type =2 then rating_ts else null end) like_w_comment_ts,
            min(case when rating_type = 5 then rating_ts else null end) match_ts,
            min(case when rating_type = 3 then rating_ts else null end) non_report_terminal_ts,
            min(case when rating_type = 4 then rating_ts else null end) report_ts,
            min(case when rating_type in (3, 4) then rating_ts else null end) terminal_ts
        from dw.fct_ratings f
        join need_update n on f.connection_id = n.connection_id
        group by f.connection_id ;''')
        return cmdList
