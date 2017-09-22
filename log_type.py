# log 각 줄을 읽기 위한 함수들

import re

# 각 테이블의 primary key를 담은 dict
primary_key = {
    'class': 'class_id',
    'course': 'course_id',
    'department': 'dept_id',
    'employee': 'ename',
    'professor': 'prof_id',
    'student': 'stu_id'
}

# log 타입을 파악하는 함수
def log_type(log):
    if re.search('<T[0-9]+> start', log):
        return 'start'
    elif re.search('<T[0-9]+> .+\..+\..+, .+, .+', log):
        return 'change value'
    elif re.search('<T[0-9]+> .+\..+\..+, .+', log):
        return 'set value'
    elif re.search('<T[0-9]+> commit', log):
        return 'commit'
    elif re.search('<T[0-9]+> abort', log):
        return 'abort'
    elif re.search('^checkpoint .+$', log):
        return 'checkpoint'

# 각 log 타입에 맞게 log 줄을 해석하여 값을 리턴하는 함수
def parse_log(log):
    type = log_type(log)
    if type == 'change value':
        transaction = re.search('^<T[0-9]+>', log).group(0)
        table, key, column = re.search('(?<=>).+\.\w+(?=,)', log).group(0).lstrip().split('.')
        old_value, new_value = re.search('(?<=,).+,.+$', log).group(0).replace(' ', '').split(',')
        return transaction, table, key, column, old_value, new_value
    elif type == 'set value':
        transaction = re.search('^<T[0-9]+>', log).group(0)
        table, key, column = re.search('(?<=>).+\.\w+(?=,)', log).group(0).lstrip().split('.')
        value = re.search('(?<=,).+$', log).group(0).lstrip()
        return transaction, table, key, column, value
    elif type == 'checkpoint':
        transactions = re.findall('<T[0-9]+>', log)
        return transactions
    else:
        transaction = re.search('<T[0-9]+>', log).group(0)
        return transaction



