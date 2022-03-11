create table if not exists
user_tags(zuhe String,sumfx DOUBLE,sumzx DOUBLE)
PARTITIONED BY (`dt` string COMMENT '月份')
row format delimited fields terminated by ',';