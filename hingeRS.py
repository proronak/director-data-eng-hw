import boto3
import psycopg2

class redshift:
    __slots__ = '__RS_PORT', '__RS_USER', '__DATABASE', '__CLUSTER_ID', '__RS_HOST'
    def __db_connection(self):

        client = boto3.client('redshift')

        cluster_creds = client.get_cluster_credentials(DbUser=self.__RS_USER,
                                                   DbName=self.__DATABASE,
                                              ClusterIdentifier=self.__CLUSTER_ID,
                                                   AutoCreate=False)

        try:
          conn = psycopg2.connect(
            host=self.__RS_HOST,
            port=self.__RS_PORT,
            user=cluster_creds['DbUser'],
            password=cluster_creds['DbPassword'],
            database=self.__DATABASE
          )
          return conn
        except psycopg2.Error:
          raise

    def __init__ (self, RS_PORT=5439, RS_USER = 'dbuser', DATABASE = 'dev', CLUSTER_ID = 'hinge-redshift-cluster', RS_HOST = 'hinge-redshift-cluster.cou0ht2ho4pg.us-east-1.redshift.amazonaws.com'):
        self.__RS_PORT = RS_PORT
        self.__RS_USER = RS_USER
        self.__DATABASE = DATABASE
        self.__CLUSTER_ID = CLUSTER_ID
        self.__RS_HOST = RS_HOST

    def db_transaction (self,cmds):
        ''' Perform DB transaction using list of cmds
        '''
        try:
            con = self.__db_connection()
        except:
            raise

        try:
            cur=con.cursor()
            for cnt,query in enumerate(cmds,0):
                cur.execute(query)

            con.commit()
        except:
            con.rollback();
            raise
        finally:
            cur.close()
            con.close()
