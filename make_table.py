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
from django.conf import settings
from lens.models import Lens
from datetime import datetime
from dateutil.relativedelta import relativedelta
today_date = datetime.today().strftime("%Y%m%d")
yesterday_date = (datetime.today()+relativedelta(days=-1)).strftime("%Y%m%d")

presto = Lens.getConnector(settings.PRESTO_LENS_NAME)
mysql = Lens.getConnector(settings.MYSQL_LENS_NAME)
hive_lens = Lens.getConnector(settings.HIVE_LENS_NAME)

# %%
from dateutil.relativedelta import relativedelta
today_date = datetime.today().strftime("%Y%m%d")
#today_date = (datetime.today()+relativedelta(days=-1)).strftime("%Y%m%d")


# %%
# query = """
# DROP TABLE cauly_dmp"""
# hive_lens.runquery(query)

# %% [markdown]
# scode는 전체 table 합집합  
# max day 확인(생성중 확인 (시간))

# %%
### 테이블 생성
table_name = 'cauly_dmp'

def create_table(table_name):
    hive_lens = Lens.getConnector(settings.HIVE_LENS_NAME)
    create_table_query = """
        create table if not exists {table_name} (
            scode string,
            segment_list array<string>
        )
        partitioned by (day string, scode_type string)
        STORED AS ORC
        location '/{table_name}/logs'
    """.format(table_name=table_name)
    print(create_table_query)
    hive_lens.runquery(create_table_query)
    
create_table(table_name)


# %%
# query = """
#     with dict as(
#     select * 
#     from cauly_dmp_dict 
#     where day = '20230703' 
#     ),
#     rt_user as(
#     select rt.deviceid as scode, segment
#     from rt_user_activity_daily3 rt
#     CROSS  join unnest(map_keys(rt.cate_map_coupang)) as t(segment)
#     where rt.day = '20230702'
#     limit 10
#     ),
#     rt_user2 as(
#     select rt_user.scode, dict.new_code as segment
#     from rt_user
#     left join dict on cast(rt_user.segment as varchar) = dict.old_code and dict.table_name = 'rt_category_info_coupang'
#     )
#     select rt_user2.scode, array_agg(rt_user2.segment) as segment
#     from rt_user2
#     group by scode
#     """
# presto.get_dataframe(query)

# %%
def get_data(today_date, yesterday_date):
    
    query = """
    insert into cauly_dmp
    with dict as(
    select * 
    from cauly_dmp_dict 
    where day = '{today_date}' 
    and exposure = 'yes'
    ),
    
    rt_user_coupang as(
    select rt.deviceid as scode, segment
    from rt_user_activity_daily3 rt
    CROSS  join unnest(map_keys(rt.cate_map_coupang)) as t(segment)
    where rt.day = '{yesterday_date}'
    ),
    rt_user_brand as(
    select rt.deviceid as scode, segment
    from rt_user_activity_daily3 rt
    CROSS  join unnest(map_keys(rt.brand_map)) as t(segment)
    where rt.day = '{yesterday_date}'
    ),
    rt_user as(
    SELECT *
    FROM rt_user_coupang 
    
    UNION ALL
    
    SELECT *
    FROM rt_user_brand
    ),
    
    rt_user2 as(
    select rt_user.scode, dict.new_code as segment
    from rt_user
    left join dict on cast(rt_user.segment as varchar) = dict.old_code and dict.table_name = 'rt_category_info_coupang'
    ),
    last_rt_user as(
    select rt_user2.scode, array_agg(rt_user2.segment) as segment
    from rt_user2
    group by scode
    ),
    aggregated_data as(
    SELECT pc.scode, concat(cast(pc.ic_segment_list as array(varchar)), cast(pc.apc_segment_list as array(varchar))) as segment
    from persona_count  pc
    where pc.day = (
        select max(day)
        from persona_count
        )
        
    UNION ALL
    
    SELECT od.scode, 
    array[CASE WHEN od.home_address = '0' THEN NULL ELSE concat('h',od.home_address) END,
    CASE WHEN od.work_address = '0' THEN NULL ELSE concat('w',od.work_address) END] as segment
    from offline_dmp_by_geo_location od
    where od.day = (
        select max(day)
        from offline_dmp_by_geo_location
        )
    
    UNION ALL
    
    select rt.scode, rt.segment 
    from last_rt_user rt

    UNION ALL
    
    select cps.scode, cast(cps.target_segments as array(varchar)) as segment
    from cauly_predict_segments cps
    where cps.day = (
        select max(day)
        from cauly_predict_segments
        )

    UNION ALL
    
    select pca.scode, array[cast(dict.new_code as varchar)] as segment
    from prediction_carrier pca
    left join dict on cast(pca.pred_carrier as varchar) = dict.old_code and dict.table_name = 'prediction_carrier'
    where pca.day = (
        select max(day)
        from prediction_carrier
        )
        
    UNION ALL
    
    select cs.scode, array_agg(cs.custom_segment) as segment
    from cauly_custom_dmp cs
    where day = '{yesterday_date}'
    group by cs.scode
    ),
    
    filter_data as(
    SELECT 
    ad.scode, 
    filter(transform(array_distinct(flatten(array_agg(ad.segment))), x -> IF(x='', NULL, x)), x -> x is not null) as segment,
    '{today_date}' day, 
    CASE
        WHEN ad.scode = upper(ad.scode) AND LENGTH(ad.scode) = 32 AND NOT REGEXP_LIKE(ad.scode, '^[0-9]+$') THEN 'idfa'
        WHEN ad.scode = lower(ad.scode) AND LENGTH(ad.scode) = 36 AND NOT REGEXP_LIKE(ad.scode, '^[0-9]+$') AND ad.scode NOT LIKE '%,%' THEN 'adid'
        END AS scode_type
    FROM aggregated_data ad
    where ad.scode NOT LIKE 'CID%'
    GROUP BY
        ad.scode
    )
    
    SELECT fd.scode, array_agg(t.segments) segment, fd.day, fd.scode_type
    from filter_data fd
    CROSS JOIN UNNEST(fd.segment) AS t(segments)
    inner join dict d on t.segments = d.new_code
    where fd.scode_type = 'idfa' or fd.scode_type = 'adid'
    group by (fd.scode, fd.day, fd.scode_type)

    
    """.format(today_date=today_date, yesterday_date=yesterday_date)
    return(presto.get_dataframe(query))


# %%
a = get_data(today_date, yesterday_date)
a

# %% [markdown]
# custom_target_segments  
# 문제점 : scode가 1일에 1개 이상, 접속 site 별로 존재 이것에 대한 정제 방법 필요  
# 해결방안 : site_id에 대한 segment 부여 > prefix로 사용해 site_id의 tsegmet cd 라는 개념의 segment로 활용
#
# > 노출하지 않기로 결정

# %% [markdown]
# segment 없는 socde 날리기  
# scode 기준 segment count(각 table 별로)  
# adid, idfa > partition(안드로이드 ios) 추가
