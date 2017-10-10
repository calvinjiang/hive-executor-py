# coding=UTF-8
from collections import OrderedDict

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

HIVE_BUILD_PARTITIONS_TEST_DATA=[]
partition=OrderedDict()
partition['dt']='20160501'
partition['hour']='00'

HIVE_BUILD_PARTITIONS_TEST_DATA.append(partition)

partition=OrderedDict()
partition['dt']=20160502
partition['hour']=1
HIVE_BUILD_PARTITIONS_TEST_DATA.append(partition)

HIVE_BUILD_PARTITIONS_TEST_RESULT=["partition(dt='20160501',hour='00')","partition(dt=20160502,hour=1)"]

HIVE_CREATE_TABLE_TEST_SQL="""
create table if not exists default.test_table_123
(
name string comment 'name',
age int comment 'age'
)
partitioned by(dt string,hour string);
"""

TABLE_DETAIL="""id                      bigint
resumeparsedid          bigint
name                    string
mobile                  string
email                   string
birthday                string
degree                  int
gender                  int
maxsalary               decimal(10,2)
minsalary               decimal(10,2)
joinworkdate            string
targetworklocations     string
targetposition          string
career                  string
projects                string
education               string
currentprovincecode     string
currentcitycode         string
currentcity             string
currentdistrictcode     string
currentdistrict         string
currentprovince         string
worktype                int
targetjobtype           string
marriage                int
registerlocation        string
politicalstatus         string
selfevaluation          string
status                  int
currentcompany          string
currentindustry         string
currentposition         string
currentsalary           string
targetindustry          string
source                  string
qq                      string
skills                  string
products                string
certificates            string
externalrefid           string
externalid              string
subject                 string
emailcity               string
emailjobtype            string
categoryfirstcodes      string
categorysecondcodes     string
categorythirdcodes      string
semanticfirstcodes      string
semanticsecondcodes     string
semanticthirdcodes      string
createtime              string
createby                string
updatetime              string
updateby                string
languages               string
trackid                 string
rdcreatetime            string                  记录创建时间
vdate                   string
vhour                   string

# Partition Information
# col_name              data_type               comment

vdate                   string
vhour                   string

Detailed Table Information  Table(tableName:s_resume_normal, dbName:stg, owner:hadoop, createTime:1488533187, lastAccessTime:0, retention:0, sd:StorageDescriptor(cols:[FieldSchema(name:id, type:bigint, comment:null), FieldSchema(name:resumeparsedid, type:bigint, comment:null), FieldSchema(name:name, type:string, comment:null), FieldSchema(name:mobile, type:string, comment:null), FieldSchema(name:email, type:string, comment:null), FieldSchema(name:birthday, type:string, comment:null), FieldSchema(name:degree, type:int, comment:null), FieldSchema(name:gender, type:int, comment:null), FieldSchema(name:maxsalary, type:decimal(10,2), comment:null), FieldSchema(name:minsalary, type:decimal(10,2), comment:null), FieldSchema(name:joinworkdate, type:string, comment:null), FieldSchema(name:targetworklocations, type:string, comment:null), FieldSchema(name:targetposition, type:string, comment:null), FieldSchema(name:career, type:string, comment:null), FieldSchema(name:projects, type:string, comment:null), FieldSchema(name:education, type:string, comment:null), FieldSchema(name:currentprovincecode, type:string, comment:null), FieldSchema(name:currentcitycode, type:string, comment:null), FieldSchema(name:currentcity, type:string, comment:null), FieldSchema(name:currentdistrictcode, type:string, comment:null), FieldSchema(name:currentdistrict, type:string, comment:null), FieldSchema(name:currentprovince, type:string, comment:null), FieldSchema(name:worktype, type:int, comment:null), FieldSchema(name:targetjobtype, type:string, comment:null), FieldSchema(name:marriage, type:int, comment:null), FieldSchema(name:registerlocation, type:string, comment:null), FieldSchema(name:politicalstatus, type:string, comment:null), FieldSchema(name:selfevaluation, type:string, comment:null), FieldSchema(name:status, type:int, comment:null), FieldSchema(name:currentcompany, type:string, comment:null), FieldSchema(name:currentindustry, type:string, comment:null), FieldSchema(name:currentposition, type:string, comment:null), FieldSchema(name:currentsalary, type:string, comment:null), FieldSchema(name:targetindustry, type:string, comment:null), FieldSchema(name:source, type:string, comment:null), FieldSchema(name:qq, type:string, comment:null), FieldSchema(name:skills, type:string, comment:null), FieldSchema(name:products, type:string, comment:null), FieldSchema(name:certificates, type:string, comment:null), FieldSchema(name:externalrefid, type:string, comment:null), FieldSchema(name:externalid, type:string, comment:null), FieldSchema(name:subject, type:string, comment:null), FieldSchema(name:emailcity, type:string, comment:null), FieldSchema(name:emailjobtype, type:string, comment:null), FieldSchema(name:categoryfirstcodes, type:string, comment:null), FieldSchema(name:categorysecondcodes, type:string, comment:null), FieldSchema(name:categorythirdcodes, type:string, comment:null), FieldSchema(name:semanticfirstcodes, type:string, comment:null), FieldSchema(name:semanticsecondcodes, type:string, comment:null), FieldSchema(name:semanticthirdcodes, type:string, comment:null), FieldSchema(name:createtime, type:string, comment:null), FieldSchema(name:createby, type:string, comment:null), FieldSchema(name:updatetime, type:string, comment:null), FieldSchema(name:updateby, type:string, comment:null), FieldSchema(name:languages, type:string, comment:null), FieldSchema(name:trackid, type:string, comment:null), FieldSchema(name:rdcreatetime, type:string, comment:记录创建时间), FieldSchema(name:vdate, type:string, comment:null), FieldSchema(name:vhour, type:string, comment:null)], location:oss://LTAI8IpG2WQ59zmQ:bXjPgYyNfZ6IBOPqU0wZxrCJQsV9bv@mf-hdfs.vpc100-oss-cn-beijing.aliyuncs.com/stg/s_resume_normal, inputFormat:org.apache.hadoop.mapred.TextInputFormat, outputFormat:org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat, compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, parameters:{serialization.format=, line.delim=
, field.delim=}), bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:false), partitionKeys:[FieldSchema(name:vdate, type:string, comment:null), FieldSchema(name:vhour, type:string, comment:null)], parameters:{EXTERNAL=TRUE, transient_lastDdlTime=1488533187}, viewOriginalText:null, viewExpandedText:null, tableType:EXTERNAL_TABLE)"""

TABLE_DETAIL_RESULT={'outputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', 'serialization.format': None, 'field.delim': '', 'sortCols': [], 'inputFormat': 'org.apache.hadoop.mapred.TextInputFormat', 'compressed': 'false', 'fields': [{'comment': 'null', 'type': 'bigint', 'name': 'id'}, {'comment': 'null', 'type': 'bigint', 'name': 'resumeparsedid'}, {'comment': 'null', 'type': 'string', 'name': 'name'}, {'comment': 'null', 'type': 'string', 'name': 'mobile'}, {'comment': 'null', 'type': 'string', 'name': 'email'}, {'comment': 'null', 'type': 'string', 'name': 'birthday'}, {'comment': 'null', 'type': 'int', 'name': 'degree'}, {'comment': 'null', 'type': 'int', 'name': 'gender'}, {'comment': 'null', 'type': 'decimal(10', 'name': 'maxsalary'}, {'comment': 'null', 'type': 'decimal(10', 'name': 'minsalary'}, {'comment': 'null', 'type': 'string', 'name': 'joinworkdate'}, {'comment': 'null', 'type': 'string', 'name': 'targetworklocations'}, {'comment': 'null', 'type': 'string', 'name': 'targetposition'}, {'comment': 'null', 'type': 'string', 'name': 'career'}, {'comment': 'null', 'type': 'string', 'name': 'projects'}, {'comment': 'null', 'type': 'string', 'name': 'education'}, {'comment': 'null', 'type': 'string', 'name': 'currentprovincecode'}, {'comment': 'null', 'type': 'string', 'name': 'currentcitycode'}, {'comment': 'null', 'type': 'string', 'name': 'currentcity'}, {'comment': 'null', 'type': 'string', 'name': 'currentdistrictcode'}, {'comment': 'null', 'type': 'string', 'name': 'currentdistrict'}, {'comment': 'null', 'type': 'string', 'name': 'currentprovince'}, {'comment': 'null', 'type': 'int', 'name': 'worktype'}, {'comment': 'null', 'type': 'string', 'name': 'targetjobtype'}, {'comment': 'null', 'type': 'int', 'name': 'marriage'}, {'comment': 'null', 'type': 'string', 'name': 'registerlocation'}, {'comment': 'null', 'type': 'string', 'name': 'politicalstatus'}, {'comment': 'null', 'type': 'string', 'name': 'selfevaluation'}, {'comment': 'null', 'type': 'int', 'name': 'status'}, {'comment': 'null', 'type': 'string', 'name': 'currentcompany'}, {'comment': 'null', 'type': 'string', 'name': 'currentindustry'}, {'comment': 'null', 'type': 'string', 'name': 'currentposition'}, {'comment': 'null', 'type': 'string', 'name': 'currentsalary'}, {'comment': 'null', 'type': 'string', 'name': 'targetindustry'}, {'comment': 'null', 'type': 'string', 'name': 'source'}, {'comment': 'null', 'type': 'string', 'name': 'qq'}, {'comment': 'null', 'type': 'string', 'name': 'skills'}, {'comment': 'null', 'type': 'string', 'name': 'products'}, {'comment': 'null', 'type': 'string', 'name': 'certificates'}, {'comment': 'null', 'type': 'string', 'name': 'externalrefid'}, {'comment': 'null', 'type': 'string', 'name': 'externalid'}, {'comment': 'null', 'type': 'string', 'name': 'subject'}, {'comment': 'null', 'type': 'string', 'name': 'emailcity'}, {'comment': 'null', 'type': 'string', 'name': 'emailjobtype'}, {'comment': 'null', 'type': 'string', 'name': 'categoryfirstcodes'}, {'comment': 'null', 'type': 'string', 'name': 'categorysecondcodes'}, {'comment': 'null', 'type': 'string', 'name': 'categorythirdcodes'}, {'comment': 'null', 'type': 'string', 'name': 'semanticfirstcodes'}, {'comment': 'null', 'type': 'string', 'name': 'semanticsecondcodes'}, {'comment': 'null', 'type': 'string', 'name': 'semanticthirdcodes'}, {'comment': 'null', 'type': 'string', 'name': 'createtime'}, {'comment': 'null', 'type': 'string', 'name': 'createby'}, {'comment': 'null', 'type': 'string', 'name': 'updatetime'}, {'comment': 'null', 'type': 'string', 'name': 'updateby'}, {'comment': 'null', 'type': 'string', 'name': 'languages'}, {'comment': 'null', 'type': 'string', 'name': 'trackid'}, {'comment': '\xe8\xae\xb0\xe5\xbd\x95\xe5\x88\x9b\xe5\xbb\xba\xe6\x97\xb6\xe9\x97\xb4', 'type': 'string', 'name': 'rdcreatetime'}, {'comment': 'null', 'type': 'string', 'name': 'vdate'}, {'comment': 'null', 'type': 'string', 'name': 'vhour'}], 'partitions': [{'comment': 'null', 'type': 'string', 'name': 'vdate'}, {'comment': 'null', 'type': 'string', 'name': 'vhour'}], 'tableName': 's_resume_normal', 'tableType': 'EXTERNAL_TABLE', 'createTime': '1488533187', 'viewOriginalText': 'null', 'location': 'oss://LTAI8IpG2WQ59zmQ:bXjPgYyNfZ6IBOPqU0wZxrCJQsV9bv@mf-hdfs.vpc100-oss-cn-beijing.aliyuncs.com/stg/s_resume_normal', 'bucketCols': [], 'numBuckets': '-1', 'owner': 'hadoop', 'viewExpandedText': 'null', 'lastAccessTime': '0', 'dbName': 'stg', 'line.delim': '', 'retention': '0'}

TABLE_FORMATTED_INFO="""
# col_name              data_type               comment

id                      bigint
resumeparsedid          bigint
name                    string
mobile                  string
email                   string
birthday                string
degree                  int
gender                  int
maxsalary               decimal(10,2)
minsalary               decimal(10,2)
joinworkdate            string
targetworklocations     string
targetposition          string
career                  string
projects                string
education               string
currentprovincecode     string
currentcitycode         string
currentcity             string
currentdistrictcode     string
currentdistrict         string
currentprovince         string
worktype                int
targetjobtype           string
marriage                int
registerlocation        string
politicalstatus         string
selfevaluation          string
status                  int
currentcompany          string
currentindustry         string
currentposition         string
currentsalary           string
targetindustry          string
source                  string
qq                      string
skills                  string
products                string
certificates            string
externalrefid           string
externalid              string
subject                 string
emailcity               string
emailjobtype            string
categoryfirstcodes      string
categorysecondcodes     string
categorythirdcodes      string
semanticfirstcodes      string
semanticsecondcodes     string
semanticthirdcodes      string
createtime              string
createby                string
updatetime              string
updateby                string
languages               string
trackid                 string

# Partition Information
# col_name              data_type               comment

vdate                   string
vhour                   string

# Detailed Table Information
Database:               stg
Owner:                  hadoop
CreateTime:             Tue Apr 18 18:30:36 CST 2017
LastAccessTime:         UNKNOWN
Retention:              0
Location:               oss://LTAI8IpG2WQ59zmQ:bXjPgYyNfZ6IBOPqU0wZxrCJQsV9bv@mf-hdfs.vpc100-oss-cn-beijing.aliyuncs.com/stg/s_resume_normal_history
Table Type:             EXTERNAL_TABLE
Table Parameters:
    EXTERNAL                TRUE
    transient_lastDdlTime   1492511436

# Storage Information
SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
InputFormat:            org.apache.hadoop.mapred.TextInputFormat
OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
Compressed:             No
Num Buckets:            96
Bucket Columns:         [id]
Sort Columns:           []
Storage Desc Params:
    field.delim             \u0001
    line.delim              \n
    serialization.format    \u0001
"""

TABLE_FORMATTED_INFO_PARSED_RESULT={'bucketColumns': ['id'], 'outputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', 'serialization.format': '\\u0001', 'field.delim': '\\u0001', 'sortColumns': [''], 'inputFormat': 'org.apache.hadoop.mapred.TextInputFormat', 'compressed': 'No', 'fields': [('id', 'bigint'), ('resumeparsedid', 'bigint'), ('name', 'string'), ('mobile', 'string'), ('email', 'string'), ('birthday', 'string'), ('degree', 'int'), ('gender', 'int'), ('maxsalary', 'decimal(10,2)'), ('minsalary', 'decimal(10,2)'), ('joinworkdate', 'string'), ('targetworklocations', 'string'), ('targetposition', 'string'), ('career', 'string'), ('projects', 'string'), ('education', 'string'), ('currentprovincecode', 'string'), ('currentcitycode', 'string'), ('currentcity', 'string'), ('currentdistrictcode', 'string'), ('currentdistrict', 'string'), ('currentprovince', 'string'), ('worktype', 'int'), ('targetjobtype', 'string'), ('marriage', 'int'), ('registerlocation', 'string'), ('politicalstatus', 'string'), ('selfevaluation', 'string'), ('status', 'int'), ('currentcompany', 'string'), ('currentindustry', 'string'), ('currentposition', 'string'), ('currentsalary', 'string'), ('targetindustry', 'string'), ('source', 'string'), ('qq', 'string'), ('skills', 'string'), ('products', 'string'), ('certificates', 'string'), ('externalrefid', 'string'), ('externalid', 'string'), ('subject', 'string'), ('emailcity', 'string'), ('emailjobtype', 'string'), ('categoryfirstcodes', 'string'), ('categorysecondcodes', 'string'), ('categorythirdcodes', 'string'), ('semanticfirstcodes', 'string'), ('semanticsecondcodes', 'string'), ('semanticthirdcodes', 'string'), ('createtime', 'string'), ('createby', 'string'), ('updatetime', 'string'), ('updateby', 'string'), ('languages', 'string'), ('trackid', 'string')], 'partitions': [('vdate', 'string'), ('vhour', 'string')], 'tableType': 'EXTERNAL_TABLE', 'lastAccessTime': 'UNKNOWN', 'location': 'oss://LTAI8IpG2WQ59zmQ:bXjPgYyNfZ6IBOPqU0wZxrCJQsV9bv@mf-hdfs.vpc100-oss-cn-beijing.aliyuncs.com/stg/s_resume_normal_history', 'numBuckets': '96', 'owner': 'hadoop', 'line.delim': '\\n', 'retention': '0'}