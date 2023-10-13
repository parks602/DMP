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
#-*- coding: utf-8 -*-

# %%
from common.querybuilder import comma_join_varchar,comma_join
from django.conf import settings
from lens.models import Lens
from common.localtimes import *
from django.db import close_old_connections
from lens.cauly.hive import add_partition, drop_partition, msck_repair, drop_table
from datetime import datetime, timedelta
import re
from hdfs import InsecureClient


close_old_connections()

presto = Lens.getConnector(settings.PRESTO_LENS_NAME)
mysql = Lens.getConnector(settings.MYSQL_LENS_NAME)
hive = Lens.getConnector(settings.HIVE_LENS_NAME)


# %%
def get_custom_dict(table_name):
    query="""
        select *
        from {table_name}
        where stat = 'use'
        """.format(table_name=table_name)
    df = presto.get_dataframe(query)
    return df

def make_custom_dmp_dmp(dict_df):
    dict_df = dict_df.reset_index(drop=True)
    insert_day = datetime.strftime(datetime.today() + timedelta(days=-1), '%Y%m%d')
    day = datetime.strftime(datetime.today() + timedelta(days=-2), '%Y%m%d')
    for i in range(len(dict_df)):
        cseg     = dict_df['custom_segment'][i]
        seg_list = dict_df['segment_list'][i]
        seg_list = [str(item) for item in seg_list]
        seg_sign = dict_df['segment_sign'][i]
        if seg_sign == 'and':
            num = len(seg_list)
        else:
            num = 1
        query="""
            insert into cauly_custom_dmp
            
            select scode, '%s' as custom_segment, '%s' as day, scode_type 
            from cauly_dmp
            where day= '%s'
            and cardinality(ARRAY_INTERSECT(segment_list, array%s))>=%i
            """%(cseg, insert_day, day, seg_list, num)
        presto.get_dataframe(query)
        print(query)
    
def make_custom_dmp_query(dict_df):
    dict_df = dict_df.reset_index(drop=True)
    insert_day = datetime.strftime(datetime.today() + timedelta(days=-1), '%Y%m%d')
    day = datetime.strftime(datetime.today() + timedelta(days=-2), '%Y%m%d')
    for i in range(len(dict_df)):
        cseg      = dict_df['custom_segment'][i]
        query_str = dict_df['query'][i]
        query     = query_str.format(day = day)
        print(query)
        querys     = """insert into cauly_custom_dmp
                    with scodes as(
                    {query})
                    select scode, '{cseg}' as custom_segment, '{insert_day}' as day,
                    CASE
                        WHEN scode = upper(scode) AND LENGTH(scode) = 32 AND NOT REGEXP_LIKE(scode, '^[0-9]+$') THEN 'idfa'
                        WHEN scode = lower(scode) AND LENGTH(scode) = 36 AND NOT REGEXP_LIKE(scode, '^[0-9]+$') AND scode NOT LIKE '%,%' THEN 'adid'
                        END AS scode_type
                    from scodes""".format(query=query, cseg=cseg, insert_day=insert_day)
        print(querys)
        df        = presto.get_dataframe(querys)

def make_custom_dmp(table_name="cauly_custom_dmp"):
    create_table_query = """
        create table if not exists {table_name} (
            scode string
        )
        partitioned by (custom_segment string, day string, scode_type string)
        STORED AS ORC
        location '/{table_name}/logs'
    """.format(table_name=table_name)
    print(create_table_query)
    hive.runquery(create_table_query)

    
def find_scode_type(value):
    if value == value.upper() and len(value) == 32 and re.match('^[0-9A-Za-z-]+$', value):
        return 'idfa'
    elif value == value.lower() and len(value) == 36 and re.match('^[0-9A-Za-z-]+$', value) and '%,%' not in value:
        return 'adid'
    else:
        return ''

# %%
# query = """
# DROP TABLE cauly_custom_dmp"""
# hive.runquery(query)

# make_custom_dmp()


# %%
make_custom_dmp()

#dmp_cauly 
custom_dict = get_custom_dict('custom_cauly_dmp_dict_with_cauly_dmp')
make_custom_dmp_dmp(custom_dict)

# %%
custom_dict = get_custom_dict('custom_cauly_dmp_dict_with_query')
make_custom_dmp_query(custom_dict)
