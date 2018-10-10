import boto3
import os

from hingeUtil import Util
from hingeRS import redshift

# constants - can be read from configuration file or environment variables too
BUCKET='hinge-hw'
SRC='input'
TGT='wip'
ULT='processed'
RS_S3_ROLE_ARN='arn:aws:iam::069013569348:role/hinge-redshift-role'
RS_PORT=5439
RS_USER = 'dbuser'
DATABASE = 'dev'
CLUSTER_ID = 'hinge-redshift-cluster'
RS_HOST = 'hinge-redshift-cluster.cou0ht2ho4pg.us-east-1.redshift.amazonaws.com'

# methods
def move_s3_key(s3Obj,srcBucket,srcKey, tgtBucket, tgtKey):
    ''' Move file from one bucket to another
    srcKey -> name including prefix
    tgtKey -> name including prefix
    '''
    try:
        copy_source = { 'Bucket': srcBucket, 'Key': srcKey }

        s3Obj.meta.client.copy(copy_source, tgtBucket, tgtKey)
        s3Obj.Object(srcBucket,srcKey).delete()
    except:
        raise

if __name__ == "__main__":

    s3 = boto3.resource('s3')
    my_bkt = s3.Bucket(BUCKET)

    for f in my_bkt.objects.filter(Prefix=SRC).all():
        if not f.key.endswith(os.sep): # Prefix is also in collection so ignore that
            dummy_f = open('dummyfile.txt','w')
            file_name = os.sep.join(f.key.split(os.sep)[len(f.key.split(os.sep))-1:])
            table_name= file_name.replace('-','_')
            tgtKey = TGT+os.sep+file_name

            move_s3_key(s3,BUCKET,f.key, BUCKET, tgtKey) # move the file out of input list to avoid pick up again
            try:
                cmds=Util.FileProcessingCmds(table_name, BUCKET, tgtKey, RS_S3_ROLE_ARN)

                rs = redshift(RS_PORT=5439, RS_USER = 'dbuser', DATABASE = 'dev', CLUSTER_ID = 'hinge-redshift-cluster', RS_HOST = 'hinge-redshift-cluster.cou0ht2ho4pg.us-east-1.redshift.amazonaws.com')
                rs.db_transaction(cmds)
            except:
                move_s3_key(s3,BUCKET, tgtKey, BUCKET, f.key) # put the file back in src as database transaction failed
                raise
            finally:
                dummy_f.close()
            move_s3_key(s3,BUCKET, tgtKey, BUCKET, ULT+os.sep+file_name)

            break # we process 1 file at a time
