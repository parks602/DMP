# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 2
#     language: python
#     name: python2
# ---

# %%
# -*- coding: utf-8 -*-
import sys

# 기본 디코딩을 UTF-8로 변경
reload(sys)
sys.setdefaultencoding('utf-8')

# %%
from django.conf import settings
from lens.models import Lens

presto = Lens.getConnector(settings.PRESTO_LENS_NAME)
mysql = Lens.getConnector(settings.MYSQL_LENS_NAME)

# %%
# query="""select tsegment_cd, SUBSTRING_INDEX(REPLACE(name,' ','') ,'>',1) as depth1, 
# TRIM(REPLACE(SUBSTRING_INDEX(name,' > ',-1),'-','')) as depth2, SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(name,' ','') ,'>',1),
# '_',-1) as keyword from target_segments where SUBSTRING_INDEX(REPLACE(name,' ','') ,'_',1) in ('RFM', '소비자성향')
# """
# df = mysql.get_dataframe(query)

# %%
# query = """select distinct(replace(SUBSTRING_INDEX(SUBSTRING_INDEX(name ,'>',1),'_',1), ' ','')) as name from target_segments where SUBSTRING_INDEX(REPLACE(name,' ','') ,'_',1) not in ('test', '테스트')"""
# df = mysql.get_dataframe(query)
# df

# %%
# for i in range (len(df)):
#     print(df['name'][i])

# %% [markdown]
#  DICT 위치 - TABLE  
#  target_segmnet - persona_count, cauly_predict_segments, custom_target_segments  
#  rt_category_info_coupang - rt_user_activity_daily3  
#  admin_code - offline_dmp_by_geo_location  
#  ?? - prediction_carrier  

# %%
tseg_dict_no = mysql.get_dataframe("""select * from target_segments where SUBSTRING_INDEX(REPLACE(name,' ','') ,'_',1) in ('RFM', '소비자성향')""")
tseg_dict_no['exposure']='no'
tseg_dict_no.head()

# %%
tseg_dict_yes = mysql.get_dataframe("""select * from target_segments where target_type  in ('APCv2','IC')""")
tseg_dict_yes['exposure'] = 'yes'
tseg_dict_yes .head()

# %%
tseg_dict_yes2 = mysql.get_dataframe("""select * from target_segments where target_type  in ('DEMO_GENDER', 'DEMO_AGE') and name LIKE '%New%'""")
tseg_dict_yes2['exposure'] = 'yes'
tseg_dict_yes = pd.concat([tseg_dict_yes, tseg_dict_yes2]).reset_index(drop=True)

# %%
td = pd.concat([tseg_dict_no[['tsegment_cd', 'description','exposure']], tseg_dict_yes[['tsegment_cd', 'description','exposure']]])
td.columns = ['new_code', 'name', 'exposure']

td.insert(0, 'old_code', td['new_code'].values)
td.insert(3, 'table', 'target_segment')
td.insert(4, 'description', '')

# %%
cate_dict = presto.get_dataframe("""select * from rt_category_info_coupang where day = '20230701'""")
cate_dict = cate_dict[cate_dict.columns[4:12]]
cate_dict.columns = list(cate_dict.columns[:-4]) + ['lv1_code', 'lv2_code', 'lv3_code', 'lv4_code']
for i in range(1,5):
    cate_dict['new_lv%s_code'%(str(i))] = '30%s'%(str(i))+cate_dict['lv%s_code'%(str(i))] 


# %%
cate_dict

# %%
# name = []
# code = []
# for i in range(1,5):
#     name = name + list(cate_dict['lv%s_name'%(str(i))].values)
#     code = code + list(cate_dict['new_lv%s_code'%(str(i))].values)


# %%
import numpy as np
cate_dict['lv4_name'][0]='셔츠/남방'

# %%
# define_cate_dict = pd.DataFrame({'name' : name, 'code' : code})

# %%
# define_cate_dict2 = define_cate_dict.drop_duplicates('code').reset_index(drop=True)
# define_cate_dict2

# %%
define_cate_dict = pd.DataFrame({'old_code' : cate_dict['lv4_code'].values, 'new_code' : cate_dict['new_lv4_code'].values, 'name' : cate_dict['lv4_name']})

# %%
define_cate_dict['table']= 'rt_category_info_coupang'
define_cate_dict['description'] = cate_dict['lv1_name']+'>'+cate_dict['lv2_name']+'>'+cate_dict['lv3_name']+'>'+cate_dict['lv4_name']
define_cate_dict['exposure']='yes'

# %%
define_cate_dict

# %%
geo_dict = presto.get_dataframe("""select * from admin_code""")
geo_dict.head()

# %%
geo_dict = geo_dict[geo_dict.columns[:4]].replace('-', '')
geo_dict = geo_dict.assign(description = geo_dict['r1']+'>'+geo_dict['r2']+'>'+geo_dict['r3'])
geo_dict = geo_dict.replace('', np.nan)

geo_dict = geo_dict.applymap(lambda x: x[:-1] if isinstance(x, str) and x.endswith('>') else x)
geo_dict = geo_dict.applymap(lambda x: x[:-1] if isinstance(x, str) and x.endswith('>') else x)

# %%
geo_dict['r2'] = geo_dict['r2'].fillna(geo_dict['r1'])
geo_dict['r3'] = geo_dict['r3'].fillna(geo_dict['r2'])

# %%
geo_dict

# %%
geo_dict['table']='admin_code'

# %%
geo_dict = geo_dict[['admin_code','r3', 'table', 'description']]
geo_dict.columns = ['new_code', 'name', 'table', 'description']
geo_dict['description'] = geo_dict['description'].str.rstrip('>') + ' (prefix h = Home address, prefix w = work address)'
geo_dict.insert(0, 'old_code', geo_dict['new_code'].values)
geo_dict['exposure']='yes'

# %%
geo_dict_h = geo_dict.copy()
geo_dict_h['new_code'] = 'h' + geo_dict_h['new_code']
geo_dict_w = geo_dict.copy()
geo_dict_w['new_code'] = 'w' + geo_dict_w['new_code']

# %%
geo_dict = pd.concat([geo_dict_h, geo_dict_w]).reset_index(drop=True)

# %%
geo_dict

# %%
code = [0,1,2,3]
new_code = [1122330, 1122331, 1122332, 1122333]
name = ['SKT', 'KT', 'LGU', 'ETC']
carrier_dict = pd.DataFrame({'old_code':code, 'new_code':new_code, 'name':name})
carrier_dict['table']='prediction_carrier'
carrier_dict['description']='112233 임의 접두사'
carrier_dict['exposure']='yes'

# %%
td.head()
td['name'] = td['name'].apply(lambda x: x.encode('utf-8')).astype(str)
td['table'] = td['table'].apply(lambda x: x.encode('utf-8')).astype(str)
td['exposure'] = td['exposure'].apply(lambda x: x.encode('utf-8')).astype(str)
td['description'] = td['description'].apply(lambda x: x.encode('utf-8')).astype(str)


# %%
td

# %%
col_list = ['name', 'table', 'exposure', 'description']
def asciitostr(df,col_list):
    for col in col_list:
        try:
            df[col]=df[col].apply(lambda x: x.encode('utf-8')).astype(str)
        except:
            continue
    return(df)
td = asciitostr(td,col_list)
define_cate_dict = asciitostr(define_cate_dict ,col_list)
geo_dict = asciitostr(geo_dict, col_list)
carrier_dict = asciitostr(carrier_dict, col_list)

# %%
td = td.astype(str)
define_cate_dict = define_cate_dict.astype(str)
geo_dict = geo_dict.astype(str)
carrier_dict = carrier_dict.astype(str)

# %%
td

# %%
all_dict = pd.concat([td, define_cate_dict, geo_dict, carrier_dict])
all_dict = all_dict.reset_index(drop=True)

# %%
all_dict = all_dict[['old_code', 'new_code', 'name', 'table', 'description', 'exposure']]
all_dict.columns = ['old_code', 'new_code', 'segment', 'table_name', 'description', 'exposure']

# %%
brand_dict = pd.read_csv('brand_map.csv', index_col=0)
brand_dict.head()

# %%
all_dict = pd.concat([all_dict, brand_dict]).reset_index(drop=True)

# %%
from datetime import datetime 
today_date = datetime.today().strftime("%Y%m%d")
all_dict['day'] = today_date

# %%
all_dict = all_dict.astype(str)

# %%
all_dict.head()

# %%
# all_dict.to_csv('ALL_DICT_%s.csv'%(today_date))

# %%
# hive_lens = Lens.getConnector(settings.HIVE_LENS_NAME)
# query = """
# DROP TABLE cauly_dmp_dict"""
# hive_lens.runquery(query)

# %%
### 테이블 생성
table_name = 'cauly_dmp_dict'

def create_table(table_name):
    hive_lens = Lens.getConnector(settings.HIVE_LENS_NAME)
    create_table_query = """
        create table if not exists {table_name} (
            old_code string,
            new_code string,
            segment string,
            table_name string,
            description string,
            exposure string
        )
        partitioned by (day string)
        STORED AS PARQUET
        location '/{table_name}/logs'
    """.format(table_name=table_name)
    print(create_table_query)
    hive_lens.runquery(create_table_query)
    
create_table(table_name)

# %%
from hdfs import InsecureClient
all_dict.to_parquet('dmp_dict.parquet')
with open('dmp_dict.parquet','rb') as reader:
    PROD_OUTPUT_DF = reader.read()

# %%
from hdfs import InsecureClient
def get_url(hosts, port):
    url = ''

    if type(hosts) == list:
        url = ';'.join(['http://' + host+':'+ str(port) for host in hosts])
    else:
        url = 'http://' + hosts + ':' + str(port)
    return url

def save_2_hdfs(df_result, set_day):

    CONF_WEBHDSF = {"host": ["cauly161.fsnsys.com", "cauly162.fsnsys.com"], "port": 50070, "user": "cauly"}
    url = get_url(CONF_WEBHDSF['host'],CONF_WEBHDSF['port'])
    print(url)
    user = CONF_WEBHDSF['user']

    client = InsecureClient(url, user=user)

    with client.write('/cauly_dmp_dict/logs/day={set_day}/result.parquet'.format(set_day=set_day), overwrite=True) as writer:
        writer.write(df_result)

save_2_hdfs(PROD_OUTPUT_DF, today_date)

# %%
from pyhive import hive
class HiveConnector:
    def __init__(self):
        '''
        Constructor
        '''
        self.param = {"username": "cauly", "host": "hmn.fsnsys.com", "port": 9100, "auth": "NOSASL"}
        self.connect()

    def connect(self):
        param = self.param
        self.conn = hive.connect(host=param['host'], port=param['port'], username=param['username'],
                                auth=param.get('auth', 'NONE'),
                                database=param.get('database', 'default'))

    def runquery(self, query):
        """
        result = self.cursor.execute(query)
        """
        try:
            if query[:-1] == ';':
                query = query[:-1]
            cursor = self.conn.cursor()
            cursor.execute(query)
        except (AttributeError, hive.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query)

        return cursor

    def add_partition(self, table_name, partition):
        """
        partition = {partition_column:partition_col_value, ...}
                  = 'partition_column1=partition_col_value1/partition_column2=partition_col_value2/...'
        partition_spec = (partition_column = partition_col_value, ...)
        """
        if type(partition)==dict:
            partition_spec = ','.join(["%s = '%s'"%(partition_column, partition_col_value) for partition_column, partition_col_value in partition.items()])
        elif type(partition)==str or type(partition)==unicode:
            partition_spec = ','.join(["%s = '%s'"%(p.split('=')[0], p.split('=')[1]) for p in partition.split('/')])
        print(partition_spec)
        self.runquery("ALTER TABLE {table} ADD IF NOT EXISTS PARTITION ({partition_spec})".format(table=table_name, partition_spec=partition_spec))


hive = HiveConnector()
hive.add_partition("cauly_dmp_dict", {"day": today_date})

# %% [markdown]
# #prediction_carrier
# 'SKT': 0
# 'KT' : 1
# 'LGU' : 2
# "ETC" : 3  
#
# custom_target_segments lable 확인  
# dict에 조회한 table명시  
# depth표시
#
