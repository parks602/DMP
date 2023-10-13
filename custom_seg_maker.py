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
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Lens 정의(python 3)

# %%
import os
import pandas as pd
from pyhive import presto, hive
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings(action='ignore')
class PrestoConnector(object):
    def __init__(self, param, source='jupyter'):
        self.param = param
        self.connect(source=source)
        
    def connect(self, source='jupyter'):
        param = self.param
        self.conn = presto.connect(host=param['host'], port=param['port'], source=source)

    def runquery(self, query):
        try:
            query = query.strip()
            if query[-1] == ';':
                query = query[:-1]
            cursor = self.conn.cursor()
            cursor.execute(query)
        except (AttributeError, pyhive.exc.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query)
        return cursor

    def get_dataframe(self, query, verbose=False):
        cursor = self.runquery(query)
        data = cursor.fetchall()
        if len(data) == 0:
            print ('row is 0')
            return
        df = pd.DataFrame(list(data))
        df.columns = [ d[0] for d in cursor.description ]
        return df
    
presto_lens =PrestoConnector({'host':'cauly161.fsnsys.com','port':8080})

# %%
import pyhive
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
        #except (AttributeError, hive.OperationalError):
        except (AttributeError, pyhive.exc.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query)

        return cursor
    
    def get_dataframe(self, query, verbose=False):
        cursor = self.runquery(query)
        data = cursor.fetchall()
        if len(data) == 0:
            print ('row is 0')
            return
        df = pd.DataFrame(list(data))
        df.columns = [ d[0] for d in cursor.description ]
        return df

hive_lens = HiveConnector()


# %% [markdown]
# ## Cauly_dmp를 이용한 custom 설계
#

# %%
def generate_query(word_list, way, day='20230731'):
    query_template = "select array_agg(new_code) segment_list\n" \
                     "from cauly_dmp_dict\n" \
                     "where day = '{day}'\n" \
                     "and ({conditions})"
    way = ' '+way+' '
    conditions = way.join(
        "(description like '%{}%' or segment like '%{}%')".format(word, word) for word in word_list
    )
   
    return query_template.format(day=day, conditions=conditions)


# %% [markdown]
# ### 예시 명품

# %%
word_list = ['구찌','프라다','보테가','발렌시아가','루이비통']
day = '20230731'
way = 'or'
seg_list = presto_lens.get_dataframe(generate_query(word_list, way, day))

# %% [markdown]
# ### 예시 운동용품

# %%
word_list = ['나이키','아디다스','언더아머','프로스펙스','아식스']
day = '20230731'
way = 'or'
seg_list = presto_lens.get_dataframe(generate_query(word_list, way, day))

# %%
seg_list

# %%
import pyhive
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
        #except (AttributeError, hive.OperationalError):
        except (AttributeError, pyhive.exc.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query)

        return cursor
    
    def get_dataframe(self, query, verbose=False):
        cursor = self.runquery(query)
        data = cursor.fetchall()
        if len(data) == 0:
            print ('row is 0')
            return
        df = pd.DataFrame(list(data))
        df.columns = [ d[0] for d in cursor.description ]
        return df
    
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


hive_lens = HiveConnector()

# %%
# query = """
# DROP TABLE custom_cauly_dmp_dict_with_cauly_dmp"""
# hive_lens.runquery(query)

# %%
### 테이블 생성
table_name = 'custom_cauly_dmp_dict_with_cauly_dmp'

def create_table(table_name):
    create_table_query = """
        create table if not exists {table_name} (
            custom_segment string,
            stat string,
            word_list array<string>,
            segment_list array<string>,
            segment_sign string
        )
        partitioned by (day string)
        STORED AS ORC
        location '/{table_name}/logs'
    """.format(table_name=table_name)
    print(create_table_query)
    hive_lens.runquery(create_table_query)
    
create_table(table_name)

# %%
table_name = 'custom_cauly_dmp_dict_with_cauly_dmp_test'

data = {
   'custom_segment' :'c0000',
    'stat' : 'O',
    'word_list': [word_list],
    'segment_list' : seg_list['segment_list'].values,
    'way' : way,
    'day' : day
}
exdf = pd.DataFrame(data)


# %%
exdf

# %%
query = """INSERT INTO {} (custom_segment, stat, word_list, segment_list, way, day)
        VALUES ('{}',
        '{}',
        array{},
        array{},
        '{}',
        '{}')""".format(table_name, 
                                              exdf['custom_segment'][0],
                                              exdf['stat'][0],
                                              exdf['word_list'][0], 
                                              exdf['segment_list'][0],
                                              exdf['way'][0],               
                                              exdf['day'][0])
presto_lens.get_dataframe(query)

# %%
print(query)

# %%
presto_lens.get_dataframe("""select * from custom_cauly_dmp_dict_with_cauly_dmp_test""".format(table_name=table_name))

# %%
presto_lens.get_dataframe("""select * from custom_cauly_dmp_dict_with_cauly_dmp_test limit 1""")


# %% [markdown]
# ### cauly dmp를 활용한 custom dict 생성 후 custom_seg의 scode 수집

# %%
def get_custom_scode():
    custom_dict = presto_lens.get_dataframe("""
    SELECT distinct(scode), custom_segment, day, scode_type
    FROM cauly_dmp
    CROSS JOIN (
        SELECT segment, custom_segment
        FROM custom_cauly_dmp_dict_with_cauly_dmp_test
        CROSS JOIN UNNEST(segment_list) AS t(segment)
        where use = 'O'

    ) custom_segments
    WHERE CONTAINS(cauly_dmp.segment_list,custom_segments.segment)
    and cauly_dmp.day = '20230730'
    """)
    return(custom_dict)
    


# %%
def get_custom_scode2():
    custom_dict = presto_lens.get_dataframe("""
    SELECT distinct(cd.scode), ct.custom_segment, cd.day, cd.scode_type
    FROM cauly_dmp cd
    INNER JOIN custom_cauly_dmp_dict_with_cauly_dmp_test ct
        on ct.use = 'O' and cardinality(ARRAY_INTERSECT(cd.segment_list,ct.segment_list))>0
    WHERE cd.day = '20230730'
    """)
    return(custom_dict)
    


# %%
presto_lens.get_dataframe("""select * from custom_cauly_dmp_dict_with_cauly_dmp_test
where use = 'O'""")

# %%
custom_data = get_custom_scode()

# %%
custom_data = get_custom_scode2()

# %%
custom_data

# %%
custom_data['custom_segment'].unique()

# %%
# custom 은 전일자로 생성해 cauly dmp를 한번에 생성하는 방향으로 하는
# 외부 table을 활용하는 방법
# 외부 table과의 시간(date) 
# 가격이랑 엮는것 (ctr 등등)
# 최종 cauly dmp에 입력하는 방법까지

# %% [markdown]
# ## 외부 table를 이용하며 전체 쿼리 저장 FORM

# %%
query="""
select deviceid as scode, 
CASE
    WHEN LOWER(platform) = 'ios' or  LOWER(platform) = 'ipados' THEN 'idfa'
    WHEN LOWER(platform) = 'android' THEN 'adid'
end as platform

from retargeting_event_storage_orc 
where day='{day}'
and cast(price AS DOUBLE) < 90000000
and cast(price AS DOUBLE) > 10000000
and deviceid != '-'
and event_name = 'purchase'
and platform != 'null'
order by price desc
"""
df = presto_lens.get_dataframe(query)
df

# %% [markdown]
# ### 구매 금액이 큰경우

# %%
query=["""
select deviceid as scode

from retargeting_event_storage_orc 
where cast(price AS DOUBLE) < 90000000
and cast(price AS DOUBLE) > 10000000
and deviceid != '-'
and event_name = 'purchase'
and platform != 'null'
and day = '%s'
""".replace("'", "''")]
day = ['20230730']

# %% [markdown]
# ### 일간 구매수가 3이상(한번에 여러개 구매하는 고객)

# %%
presto_lens.get_dataframe("""
SELECT deviceid AS scode, COUNT(*) AS repeat_count
FROM retargeting_event_storage_orc
WHERE deviceid != '-'
    AND event_name = 'purchase'
    AND platform != 'null'
    AND day = '20230730'
GROUP BY deviceid
HAVING COUNT(*) >= 3
""")

# %%
query=["""
SELECT deviceid AS scode
FROM retargeting_event_storage_orc
WHERE deviceid != '-'
    AND event_name = 'purchase'
    AND platform != 'null'
    and day > '%s' YYYYmmdd -- date format
    and day < cast(%s as date) - 7 day 
GROUP BY deviceid
HAVING COUNT(*) >= 3

""".replace("'", "''")]
day = ['20230730']

# %%
query=["""
select deviceid as scode

from retargeting_event_storage_orc 
where cast(price AS DOUBLE) < 90000000
and cast(price AS DOUBLE) > 10000000
and deviceid != '-'
and event_name = 'purchase'
and platform != 'null'
and day = '%s'
""".replace("'", "''")]
day = ['20230730']

# %%
import pandas as pd
data = {
   'custom_segment' :'c0001',
    'stat' : 'O',
    'query': query,
    'day' : day
}
exdf = pd.DataFrame(data)
exdf

# %%
print(exdf['query'][0])

# %%
# query = """
# DROP TABLE custom_cauly_dmp_dict_with_query_test"""
# hive_lens.runquery(query)

# %%
### 테이블 생성
table_name = 'custom_cauly_dmp_dict_with_query_test'

def create_table(table_name):
    create_table_query = """
        create table if not exists {table_name} (
            custom_segment string,
            stat string,
            query string
        )
        partitioned by (day string)
        STORED AS ORC
        location '/{table_name}/logs'
    """.format(table_name=table_name)
    print(create_table_query)
    hive_lens.runquery(create_table_query)
    
create_table(table_name)

# %%
querys = """INSERT INTO {} (custom_segment, stat, query, day)
        VALUES ('{}', '{}', '{}', '{}')""".format(table_name, exdf['custom_segment'][0],
                                              exdf['stat'][0], 
                                              exdf['query'][0],
                                              exdf['day'][0])
presto_lens.get_dataframe(querys)

# %%
querys

# %%
presto_lens.get_dataframe("""select * from custom_cauly_dmp_dict_with_query_test""")

# %%
df = presto_lens.get_dataframe("""select * from custom_cauly_dmp_dict_with_query_test where use = 'O'""")
df

# %%
df['query'][0]

# %%
print(df['query'][0])


# %% [markdown]
# ### Scode type 입력기

# %%
def determine_scode_type(row):
    if row['scode'].isupper() and len(row['scode']) == 32 and not row['scode'].isdigit():
        return 'idfa'
    elif row['scode'].islower() and len(row['scode']) == 36 and not row['scode'].isdigit() and ',' not in row['scode']:
        return 'adid'
    else:
        return None 


# %%
day = '20230730'

    
def get_scode_from_query(df, day):
    for index, row in df.iterrows():
        new_query = row['query']%(day)
        print(new_query)
        data = presto_lens.get_dataframe(new_query)
        data['custom_segment'] = row['custom_segment']
        data['day'] = '20230731'
        data['scode_type'] = data.apply(determine_scode_type, axis=1)
        print(data.shape)
        if index==0:
            dataset = data
        else:
            dataset = pd.concat([dataset,data]).reset_index(drop=True)

    return(dataset)


# %%
custom_dataset_query = get_scode_from_query(df, day)

# %%
custom_dataset_query

# %%
custom_data

# %% [markdown]
# ## 두 custom table 병합

# %%
merged_df = pd.concat([custom_data, custom_dataset_query])
grouped_df = merged_df.groupby(['scode', 'day', 'scode_type'])['custom_segment'].apply(list).reset_index()


# %%
merged_df.shape

# %%
grouped_df.head()

# %%
grouped_df=grouped_df[['scdoe', ]]

# %% [markdown]
# ## cauly_custom_dmp_test 생성 및 custom data 입력

# %%
# query = """
# DROP TABLE cauly_custom_dmp_test"""
# hive_lens.runquery(query)

# %%
### 테이블 생성
table_name = 'cauly_custom_dmp_test'

def create_table(table_name):
    create_table_query = """
        create table if not exists {table_name} (
            scode string,
            segment_list array<string>,
            scode_type string
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

def get_url(hosts, port):
    url = ''

    if type(hosts) == list:
        url = ';'.join(['http://' + host+':'+ str(port) for host in hosts])
    else:
        url = 'http://' + hosts + ':' + str(port)
    return url

def save_2_hdfs(df_result, set_day, table_name):

    CONF_WEBHDSF = {"host": ["cauly161.fsnsys.com", "cauly162.fsnsys.com"], "port": 50070, "user": "cauly"}
    url = get_url(CONF_WEBHDSF['host'],CONF_WEBHDSF['port'])
    user = CONF_WEBHDSF['user']

    client = InsecureClient(url, user=user)

    with client.write('/{table_name}/logs/day={set_day}/result.parquet'.format(table_name=table_name,set_day=set_day), overwrite=True) as writer:
        writer.write(df_result)


# %%
grouped_df.to_parquet('result.parquet')
with open('result.parquet','rb') as reader:
    PROD_OUTPUT_DF = reader.read()

save_2_hdfs(PROD_OUTPUT_DF, day, table_name)

# %%
hive_lens.add_partition(table_name, {"day": day})
