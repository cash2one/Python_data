#!/usr/bin/env python
#-*- coding: utf-8 -*-

#配置:库名->表名->字段
table = {
    'cube': {
        'sem_360_diyu': {'columns': ['stat_date', 'accountName', 'cityName', 'provinceName', 'groupId', 'campaignId', 'groupName', 'campaignName', 'clicks', 'views', 'totalCost', 'platform']}
                    }
        }

#if __name__ == '__main__':
#    # config test
#    # >1的列表
#    gt_1 = []
#    for db in table:
#        for table_t in table[db]:
#            columns = table[db][table_t]['columns']
#            print db + '.' + table_t, '========>', columns
#            for column in columns:
#                if columns.count(column) > 1 and column != 'NULL':
#                    gt_1.append({table_t: {'db': db, 'column': column}})
#
#    print gt_1

