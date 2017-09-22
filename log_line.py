# Log 파일의 각 줄의 타입(start, abort 등)에 해당하는 클래스와
# Log 파일의 모든 줄을 읽고 복구하는 클래스

from abc import ABC, abstractmethod
from log_type import *
from cursor import connection, cursor
import weakref

# log 각 줄에 할당하기 위한 abstract 클래스 및 자식 클래스
class logLine(ABC):

    def __init__(self, log):
        self.type = log_type(log)

    @abstractmethod
    def redo(self):
        pass

    @abstractmethod
    def undo(self):
        pass


class logChangeValue(logLine):

    def __init__(self, log):
        super().__init__(log)
        self.transaction, self.table, self.key, self.column, self.old_value, self.new_value = parse_log(log)
        self.pk = primary_key[self.table]

    def redo(self):
        global connection, cursor
        sql = "UPDATE %s SET %s = '%s' WHERE %s = '%s'" % (self.table, self.column, self.new_value, self.pk, self.key)
        cursor.execute(sql)

    def undo(self):
        global connection, cursor, recoveryLogs
        if self.transaction in recoveryLogs.instances[0].undo_list:
            sql = "UPDATE %s SET %s = '%s' WHERE %s = '%s'" % (self.table, self.column, self.old_value, self.pk, self.key)
            cursor.execute(sql)
            new_log = '\n' + self.transaction + ' ' + '.'.join([self.table, self.key, self.column]) + ", " + self.old_value
            recoveryLogs.instances[0].log_file.write(new_log)


class logSetValue(logLine):

    def __init__(self, log):
        super().__init__(log)
        self.transaction, self.table, self.key, self.column, self.value = parse_log(log)
        self.pk = primary_key[self.table]

    def redo(self):
        global connection, cursor
        sql = "UPDATE %s SET %s = '%s' WHERE %s = '%s'" % (self.table, self.column, self.value, self.pk, self.key)
        cursor.execute(sql)

    def undo(self):
        pass


class logCheckpoint(logLine):

    def __init__(self, log):
        super().__init__(log)
        self.transactions = parse_log(log)

    def redo(self):
        pass

    def undo(self):
        pass


class logStart(logLine):

    def __init__(self, log):
        super().__init__(log)
        self.transaction = parse_log(log)

    def redo(self):
        global recoveryLogs
        recoveryLogs.instances[0].undo_list.append(self.transaction)

    def undo(self):
        global recoveryLogs
        if self.transaction in recoveryLogs.instances[0].undo_list:
            recoveryLogs.instances[0].undo_list.remove(self.transaction)
            new_log = '\n' + self.transaction + ' abort'
            recoveryLogs.instances[0].log_file.write(new_log)



class logCommit(logLine):

    def __init__(self, log):
        super().__init__(log)
        self.transaction = parse_log(log)

    def redo(self):
        global recoveryLogs
        recoveryLogs.instances[0].undo_list.remove(self.transaction)

    def undo(self):
        pass


class logAbort(logLine):

    def __init__(self, log):
        super().__init__(log)
        self.transaction = parse_log(log)

    def redo(self):
        global recoveryLogs
        recoveryLogs.instances[0].undo_list.remove(self.transaction)

    def undo(self):
        pass

# log 각 줄을 읽고 redo와 undo를 수행하기 위한 클래스
class recoveryLogs():

    instances = []

    def __init__(self):
        self.__class__.instances.append(weakref.proxy(self))
        self.log_file = None
        self.log_lines = []
        self.undo_list = []

    def read_log(self):
        self.log_file = open('recovery.txt', 'r', encoding='utf-8')
        for line in self.log_file:
            line_type = log_type(line)
            self.log_lines.append(
                eval('log' + line_type.title().replace(' ', '') + '(line)')
            )
        self.log_file.close()

    def recover(self):
        self.log_file = open('recovery.txt', 'a', encoding='utf-8')
        for i, line in enumerate(self.log_lines):
            if line.type == 'checkpoint':
                checkpoint_index = i
                self.undo_list += line.transactions
                break

        # Redo phase
        for line in self.log_lines[checkpoint_index:]:
            line.redo()


        # Undo phase
        for line in self.log_lines[::-1]:
            line.undo()
            if not self.undo_list:
                break

        global connection
        connection.commit()
        connection.close()

        self.log_file.close()