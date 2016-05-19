# coding=UTF-8

HIVE_PARTITIONS_TEST_DATA="""dt=20160508/hour=00/type=click
dt=20160508/hour=00/type=deliver
dt=20160508/hour=00/type=realdeliver
dt=20160508/hour=00/type=realreq
dt=20160508/hour=01/type=click
dt=20160508/hour=01/type=deliver
dt=20160508/hour=01/type=realdeliver
dt=20160508/hour=01/type=realreq
dt=20160508/hour=02/type=click
dt=20160508/hour=02/type=deliver
dt=20160508/hour=02/type=realdeliver
dt=20160508/hour=02/type=realreq
dt=20160508/hour=03/type=click
dt=20160508/hour=03/type=deliver
dt=20160508/hour=03/type=realdeliver
dt=20160508/hour=03/type=realreq
dt=20160508/hour=04/type=click
dt=20160508/hour=04/type=deliver
dt=20160508/hour=04/type=realdeliver
dt=20160508/hour=04/type=realreq
dt=20160508/hour=05/type=click
dt=20160508/hour=05/type=deliver
dt=20160508/hour=05/type=realdeliver
dt=20160508/hour=05/type=realreq
dt=20160508/hour=06/type=click
dt=20160508/hour=06/type=deliver
dt=20160508/hour=06/type=realdeliver
dt=20160508/hour=06/type=realreq
dt=20160508/hour=07/type=click
dt=20160508/hour=07/type=deliver
dt=20160508/hour=07/type=realdeliver
dt=20160508/hour=07/type=realreq
dt=20160508/hour=08/type=click
dt=20160508/hour=08/type=deliver
dt=20160508/hour=08/type=realdeliver
dt=20160508/hour=08/type=realreq
dt=20160508/hour=09/type=click
dt=20160508/hour=09/type=deliver
dt=20160508/hour=09/type=realdeliver
dt=20160508/hour=09/type=realreq
dt=20160508/hour=10/type=click
dt=20160508/hour=10/type=deliver
dt=20160508/hour=10/type=realdeliver
dt=20160508/hour=10/type=realreq
dt=20160508/hour=11/type=click
dt=20160508/hour=11/type=deliver
dt=20160508/hour=11/type=realdeliver
dt=20160508/hour=11/type=realreq
dt=20160508/hour=12/type=click
dt=20160508/hour=12/type=deliver
dt=20160508/hour=12/type=realdeliver
dt=20160508/hour=12/type=realreq
dt=20160508/hour=13/type=click
dt=20160508/hour=13/type=deliver
dt=20160508/hour=13/type=realdeliver
dt=20160508/hour=13/type=realreq
dt=20160508/hour=14/type=click
dt=20160508/hour=14/type=deliver
dt=20160508/hour=14/type=realdeliver
dt=20160508/hour=14/type=realreq
dt=20160508/hour=15/type=click
dt=20160508/hour=15/type=deliver
dt=20160508/hour=15/type=realdeliver
dt=20160508/hour=15/type=realreq
dt=20160508/hour=16/type=click
dt=20160508/hour=16/type=deliver
dt=20160508/hour=16/type=realdeliver
dt=20160508/hour=16/type=realreq
dt=20160508/hour=17/type=click
dt=20160508/hour=17/type=deliver
dt=20160508/hour=17/type=realdeliver
dt=20160508/hour=17/type=realreq
dt=20160508/hour=18/type=click
dt=20160508/hour=18/type=deliver
dt=20160508/hour=18/type=realdeliver
dt=20160508/hour=18/type=realreq
dt=20160508/hour=19/type=click
dt=20160508/hour=19/type=deliver
dt=20160508/hour=19/type=realdeliver
dt=20160508/hour=19/type=realreq
dt=20160508/hour=20/type=click
dt=20160508/hour=20/type=deliver
dt=20160508/hour=20/type=realdeliver
dt=20160508/hour=20/type=realreq
dt=20160508/hour=21/type=click
dt=20160508/hour=21/type=deliver
dt=20160508/hour=21/type=realdeliver
dt=20160508/hour=21/type=realreq
dt=20160508/hour=22/type=click
dt=20160508/hour=22/type=deliver
dt=20160508/hour=22/type=realdeliver
dt=20160508/hour=22/type=realreq
dt=20160508/hour=23/type=click
dt=20160508/hour=23/type=deliver
dt=20160508/hour=23/type=realdeliver
dt=20160508/hour=23/type=realreq"""

HIVE_PARTITIONS_TEST_DATA_PARSE_RESULT=[{'dt':'20160508','hour':'00','type':'click'},{'dt':'20160508','hour':'00','type':'deliver'},{'dt':'20160508','hour':'00','type':'realdeliver'},{'dt':'20160508','hour':'00','type':'realreq'},{'dt':'20160508','hour':'01','type':'click'},{'dt':'20160508','hour':'01','type':'deliver'},{'dt':'20160508','hour':'01','type':'realdeliver'},{'dt':'20160508','hour':'01','type':'realreq'},{'dt':'20160508','hour':'02','type':'click'},{'dt':'20160508','hour':'02','type':'deliver'},{'dt':'20160508','hour':'02','type':'realdeliver'},{'dt':'20160508','hour':'02','type':'realreq'},{'dt':'20160508','hour':'03','type':'click'},{'dt':'20160508','hour':'03','type':'deliver'},{'dt':'20160508','hour':'03','type':'realdeliver'},{'dt':'20160508','hour':'03','type':'realreq'},{'dt':'20160508','hour':'04','type':'click'},{'dt':'20160508','hour':'04','type':'deliver'},{'dt':'20160508','hour':'04','type':'realdeliver'},{'dt':'20160508','hour':'04','type':'realreq'},{'dt':'20160508','hour':'05','type':'click'},{'dt':'20160508','hour':'05','type':'deliver'},{'dt':'20160508','hour':'05','type':'realdeliver'},{'dt':'20160508','hour':'05','type':'realreq'},{'dt':'20160508','hour':'06','type':'click'},{'dt':'20160508','hour':'06','type':'deliver'},{'dt':'20160508','hour':'06','type':'realdeliver'},{'dt':'20160508','hour':'06','type':'realreq'},{'dt':'20160508','hour':'07','type':'click'},{'dt':'20160508','hour':'07','type':'deliver'},{'dt':'20160508','hour':'07','type':'realdeliver'},{'dt':'20160508','hour':'07','type':'realreq'},{'dt':'20160508','hour':'08','type':'click'},{'dt':'20160508','hour':'08','type':'deliver'},{'dt':'20160508','hour':'08','type':'realdeliver'},{'dt':'20160508','hour':'08','type':'realreq'},{'dt':'20160508','hour':'09','type':'click'},{'dt':'20160508','hour':'09','type':'deliver'},{'dt':'20160508','hour':'09','type':'realdeliver'},{'dt':'20160508','hour':'09','type':'realreq'},{'dt':'20160508','hour':'10','type':'click'},{'dt':'20160508','hour':'10','type':'deliver'},{'dt':'20160508','hour':'10','type':'realdeliver'},{'dt':'20160508','hour':'10','type':'realreq'},{'dt':'20160508','hour':'11','type':'click'},{'dt':'20160508','hour':'11','type':'deliver'},{'dt':'20160508','hour':'11','type':'realdeliver'},{'dt':'20160508','hour':'11','type':'realreq'},{'dt':'20160508','hour':'12','type':'click'},{'dt':'20160508','hour':'12','type':'deliver'},{'dt':'20160508','hour':'12','type':'realdeliver'},{'dt':'20160508','hour':'12','type':'realreq'},{'dt':'20160508','hour':'13','type':'click'},{'dt':'20160508','hour':'13','type':'deliver'},{'dt':'20160508','hour':'13','type':'realdeliver'},{'dt':'20160508','hour':'13','type':'realreq'},{'dt':'20160508','hour':'14','type':'click'},{'dt':'20160508','hour':'14','type':'deliver'},{'dt':'20160508','hour':'14','type':'realdeliver'},{'dt':'20160508','hour':'14','type':'realreq'},{'dt':'20160508','hour':'15','type':'click'},{'dt':'20160508','hour':'15','type':'deliver'},{'dt':'20160508','hour':'15','type':'realdeliver'},{'dt':'20160508','hour':'15','type':'realreq'},{'dt':'20160508','hour':'16','type':'click'},{'dt':'20160508','hour':'16','type':'deliver'},{'dt':'20160508','hour':'16','type':'realdeliver'},{'dt':'20160508','hour':'16','type':'realreq'},{'dt':'20160508','hour':'17','type':'click'},{'dt':'20160508','hour':'17','type':'deliver'},{'dt':'20160508','hour':'17','type':'realdeliver'},{'dt':'20160508','hour':'17','type':'realreq'},{'dt':'20160508','hour':'18','type':'click'},{'dt':'20160508','hour':'18','type':'deliver'},{'dt':'20160508','hour':'18','type':'realdeliver'},{'dt':'20160508','hour':'18','type':'realreq'},{'dt':'20160508','hour':'19','type':'click'},{'dt':'20160508','hour':'19','type':'deliver'},{'dt':'20160508','hour':'19','type':'realdeliver'},{'dt':'20160508','hour':'19','type':'realreq'},{'dt':'20160508','hour':'20','type':'click'},{'dt':'20160508','hour':'20','type':'deliver'},{'dt':'20160508','hour':'20','type':'realdeliver'},{'dt':'20160508','hour':'20','type':'realreq'},{'dt':'20160508','hour':'21','type':'click'},{'dt':'20160508','hour':'21','type':'deliver'},{'dt':'20160508','hour':'21','type':'realdeliver'},{'dt':'20160508','hour':'21','type':'realreq'},{'dt':'20160508','hour':'22','type':'click'},{'dt':'20160508','hour':'22','type':'deliver'},{'dt':'20160508','hour':'22','type':'realdeliver'},{'dt':'20160508','hour':'22','type':'realreq'},{'dt':'20160508','hour':'23','type':'click'},{'dt':'20160508','hour':'23','type':'deliver'},{'dt':'20160508','hour':'23','type':'realdeliver'},{'dt':'20160508','hour':'23','type':'realreq'}]

HIVE_TABLES_TEST_DATA="""adm_admind_log_base
adm_app_flw_site_base_d_sum
adm_app_new_user_d_sum
adm_app_user_flw_d_sum
adm_cntt_clb_user_inter_d_sum
adm_ec_flw_pv_d_sum
adm_ec_ord_d_sum
adm_flw_autoindex_data_d_sum
adm_flw_autoindex_data_time_d_sum
adm_flw_automallsp_mid_d_sum
adm_flw_pvlog_business_d_sum
adm_flw_time_series_city_d_sum
adm_flw_time_series_city_d_sum_2014
adm_flw_time_session_sitecategorysub_d_sum
adm_led_club_ord_d_sum
adm_led_club_ord_deal_d_sum
adm_led_trans_d_sum
adm_pe_item_statistic_day_w_sum
adm_series_flw_distr_d_fact
adm_usr_club_login_log_d_sum
adm_uv_city_ver_d_sum"""


HIVE_TABLES_TEST_DATA_PARSE_RESULT=['adm_admind_log_base','adm_app_flw_site_base_d_sum','adm_app_new_user_d_sum','adm_app_user_flw_d_sum','adm_cntt_clb_user_inter_d_sum','adm_ec_flw_pv_d_sum','adm_ec_ord_d_sum','adm_flw_autoindex_data_d_sum','adm_flw_autoindex_data_time_d_sum','adm_flw_automallsp_mid_d_sum','adm_flw_pvlog_business_d_sum','adm_flw_time_series_city_d_sum','adm_flw_time_series_city_d_sum_2014','adm_flw_time_session_sitecategorysub_d_sum','adm_led_club_ord_d_sum','adm_led_club_ord_deal_d_sum','adm_led_trans_d_sum','adm_pe_item_statistic_day_w_sum','adm_series_flw_distr_d_fact','adm_usr_club_login_log_d_sum','adm_uv_city_ver_d_sum']


HIVE_DESC_TABLE_TEST_DATA="""deliver_time           string                  广告曝光日志时间yyyymmddhh
adpos_id                string                  广告位Id
platform_id             string                  广告平台Id,区分web m端 app
region                  string                  城市或省Id
browser                 string                  浏览器
browser_version         string                  浏览器版本
flash_enable            string                  Flash插件标识
network                 string                  网络环境
app_id                  string                  APP标识m端使用os，app使用pm
device_brand            string                  设备品牌
screen                  string                  屏幕分辨率
campaign_id             string                  广告计划标识
creative_id             string                  素材唯一标识
billing_type            string                  计费类型
carousel_id             string                  轮播号
campaign_adpos_id       string                  计划广告位标识
user_info               string                  用户的属性信息
page_info               string                  页面的属性信息
match_user_info         string                  匹配命中用户属性，前端实时计算
match_page_info         string                  匹配命中页面属性，前端实时计算
user_choice_info        string                  用户选择的属性信息
match_user_choice_info  string                  匹配命中用户选择的属性信息
car_word_type           string                  关键词类型
car_word_code           string                  关键词编码
ad_product_type         string                  品专产品类型
template_type           string                  广告位Id
deliver_creative_type   string                  素材曝光形式
uv_id                   string                  浏览cookie中的sessionid或者app中的设备id
ip                      string                  浏览IP
user_id                 string                  用户的登录id
img_type                string                  素材类型 app开屏广告使用:gif jpg
click_position          string                  广告点击位置
landing_page_url        string                  转化landingpage
engine_rt               bigint                  广告引擎响应时长
realreq_staytime        bigint                  可见请求停留时长
realdeliver_staytime    bigint                  可见曝光停留时长
pages                   bigint                  页面数
adreqnum                bigint                  广告曝光数
filter_clicks           bigint                  作弊点击数
clicks                  bigint                  点击数
narrow_lands            bigint                  狭义的转化数
broad_lands             bigint                  广义的转化数
lands                   bigint                  转化数量
deliver_filter_pv       bigint                  曝光作弊PV
deliver_pv              bigint                  曝光PV
realreq_pv              bigint                  可见请求PV
realdeliver_pv          bigint                  可见曝光PV
realdeliver_unique_pv   bigint                  可见曝光独立PV
click_filter_pv         bigint                  点击作弊PV
click_pv                bigint                  点击日志PV
lands_pv                bigint                  转化日志PV
dt                      string
hour                    string

# Partition Information
# col_name              data_type               comment

dt                      string
hour                    string"""

HIVE_DESC_TABLE_TEST_RESULT={'fields':[
('deliver_time','string','广告曝光日志时间yyyymmddhh'),
('adpos_id','string','广告位Id'),
('platform_id','string','广告平台Id,区分web m端 app'),
('region','string','城市或省Id'),
('browser','string','浏览器'),
('browser_version','string','浏览器版本'),
('flash_enable','string','Flash插件标识'),
('network','string','网络环境'),
('app_id','string','APP标识m端使用os，app使用pm'),
('device_brand','string','设备品牌'),
('screen','string','屏幕分辨率'),
('campaign_id','string','广告计划标识'),
('creative_id','string','素材唯一标识'),
('billing_type','string','计费类型'),
('carousel_id','string','轮播号'),
('campaign_adpos_id','string','计划广告位标识'),
('user_info','string','用户的属性信息'),
('page_info','string','页面的属性信息'),
('match_user_info','string','匹配命中用户属性，前端实时计算'),
('match_page_info','string','匹配命中页面属性，前端实时计算'),
('user_choice_info','string','用户选择的属性信息'),
('match_user_choice_info','string','匹配命中用户选择的属性信息'),
('car_word_type','string','关键词类型'),
('car_word_code','string','关键词编码'),
('ad_product_type','string','品专产品类型'),
('template_type','string','广告位Id'),
('deliver_creative_type','string','素材曝光形式'),
('uv_id','string','浏览cookie中的sessionid或者app中的设备id'),
('ip','string','浏览IP'),
('user_id','string','用户的登录id'),
('img_type','string','素材类型 app开屏广告使用:gif jpg'),
('click_position','string','广告点击位置'),
('landing_page_url','string','转化landingpage'),
('engine_rt','bigint','广告引擎响应时长'),
('realreq_staytime','bigint','可见请求停留时长'),
('realdeliver_staytime','bigint','可见曝光停留时长'),
('pages','bigint','页面数'),
('adreqnum','bigint','广告曝光数'),
('filter_clicks','bigint','作弊点击数'),
('clicks','bigint','点击数'),
('narrow_lands','bigint','狭义的转化数'),
('broad_lands','bigint','广义的转化数'),
('lands','bigint','转化数量'),
('deliver_filter_pv','bigint','曝光作弊PV'),
('deliver_pv','bigint','曝光PV'),
('realreq_pv','bigint','可见请求PV'),
('realdeliver_pv','bigint','可见曝光PV'),
('realdeliver_unique_pv','bigint','可见曝光独立PV'),
('click_filter_pv','bigint','点击作弊PV'),
('click_pv','bigint','点击日志PV'),
('lands_pv','bigint','转化日志PV'),
('dt','string'),
('hour','string')],
'partitions':[('dt','string'),('hour','string')]}


HIVE_DESC_DATABASE_TEST_DATA="""
db_name comment location    owner_name  owner_type  parameters
adm     hdfs://AutoLqCluster/group_sjpt/hive_db/adm.db  root    USER"""

HIVE_DESC_DATABASE_TEST_RESULT={"db_name":"adm","comment":"","location":"hdfs://AutoLqCluster/group_sjpt/hive_db/adm.db","owner_name":"root","owner_type":"USER","parameters":""}

