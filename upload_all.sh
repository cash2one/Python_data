##########################################################
## 功能：从mysql导数据到hive
##########################################################

## 全表
table="
d_app_tuig_xiaofei_conf
d_tuig_qudao_conf
d_page_flow_conf
d_wx_menu_conf
d_org_conf
d_sem_xiaofei_conf
d_bd_xiaofei_conf
d_wx_xiaofei_conf
d_tuiguang_conf
d_tuigfei_conf
d_app_xiaofei_conf
d_sem_citytype_conf
d_app_tinyurl_conf
d_zhucai_zengxiang
d_page_flow_conf
d_huodong_flow_conf
d_kefu_architecture_conf
d_sem_zht_xiaofei_conf
"

mysql_comm=`cat "/data1/bi/datacenter/rep/conf/mysql_comm.txt"`    # mysql_comm = mysql -h192.168.11.13 -udatacenter -pLoveTo8toData -Dto8to_result --default-character-set=utf8
dir="/data1/bi/datacenter/logs/data"

for table_name in `echo $table`
do
    echo $table_name
    echo "select * from to8to_result.${table_name}" | $mysql_comm | sed '1d' > ${dir}/${table_name}.txt  #当你使用重定向">"而不是">>"指定一个文件时，该文件时要被清空重新填入数据
    cnt=`wc -l ${dir}/${table_name}.txt | awk -F" " '{print $1 }' `   # 【wc -l file】 显示一个文件的行数 【awk -F" " '{print $1 }'】 $1指指定分隔符后取第一个字段
    if [ $cnt -le 0 ]
    then
        continue
    fi

    /usr/bin/hive -e "load data local inpath \"${dir}/${table_name}.txt\" overwrite into table to8to_result.$table_name" 

    rm ${dir}/${table_name}.txt

done

##分区表
table="
"
date_curr=`date -d "$date_begin 1 day ago" "+%Y%m%d"`

mysql_comm=`cat "/data1/bi/datacenter/rep/conf/mysql_comm.txt"`
dir="/data1/bi/datacenter/logs/data"
for table_name in `echo $table`
do
    echo $table_name
    echo "select * from to8to_result.${table_name} where stat_date=$date_curr" | $mysql_comm | sed '1d' > ${dir}/${table_name}.txt
    cnt=`wc -l ${dir}/${table_name}.txt | awk -F" " '{print $1 }' `
    if [ $cnt -le 0 ]   #小于等于0
    then
        exit
    fi

    /usr/bin/hive -e "load data local inpath \"${dir}/${table_name}.txt\" overwrite into table to8to_result.$table_name partition (dt=$date_curr)" 

    rm ${dir}/${table_name}.txt

done

date_curr=`date +%Y%m%d-%H:%M:%S`
echo $date_curr
