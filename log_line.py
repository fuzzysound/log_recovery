from abc import ABC, abstractmethod
from log_type import *
from cursor import connection, cursor
import weakref


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
        recoveryLogs.instances[0].undo_list.remove(self.transaction)


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


class recoveryLogs():

    instances = []

    def __init__(self):
        self.__class__.instances.append(weakref.proxy(self))
        self.log_lines = []
        self.undo_list = []

    def read_log(self):
        with open('recovery.txt', encoding='utf-8') as log_file:
            for line in log_file:
                line_type = log_type(line)
                self.log_lines.append(
                    eval('log' + line_type.title().replace(' ', '') + '(line)')
                )

    def recover(self):
        for i, line in enumerate(self.log_lines):
            if line.type == 'checkpoint':
                checkpoint_index = i
                self.undo_list += line.transactions
                break

        # Redo phase
        for line in self.log_lines[checkpoint_index:]:
            line.redo()


        # Undo phase
        for line in self.log_lines[:checkpoint_index:-1]:
            line.undo()
            if not self.undo_list:
                break

        global connection
        connection.commit()
        connection.close()