import sqlite3

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SQLiteDb(object):

    def __init__(self, database_name, factory=dict_factory):

        self.__db = sqlite3.connect(database_name)
        self.__db.row_factory = factory

        self.__cursor = self.__db.cursor()

    def __sql_execute__(self, sql_statement, args=None):
        cursor = self.__cursor
        try:
            if args is not None:
                cursor.execute(sql_statement, args)
            else:
                cursor.execute(sql_statement)

            result = cursor.fetchall()
        except sqlite3.DatabaseError as err:
            return False
        else:
            self.__db.commit()
            return result

    def check_exist_user(self, user_id):
        user = self.__sql_execute__(
            "SELECT * FROM users WHERE user_id = ?",
            (
                user_id,
            )
        )

        return True if len(user) != 0 else False

    def check_exist_group(self, group_id):
        group = self.__sql_execute__(
            "SELECT * FROM groups WHERE group_id = ?",
            (
                group_id,
            )
        )

        return True if len(group) != 0 else False

    def check_exist_connection(self, group_id, user_id):
        connection = self.__sql_execute__(
            "SELECT * FROM connection WHERE group_id = ? AND user_id = ?",
            (
                group_id,
                user_id
            )
        )

        return True if len(connection) != 0 else False

    def create_user(self, user_id, first_name, last_name, count=0):
        result = self.__sql_execute__(
            '''INSERT INTO users (user_id, firstName, lastName, count)
              VALUES (?, ?, ?, ?)''',
            (
                user_id,
                first_name,
                last_name,
                count
            )
        )

        return True if type(result) == list else False

    def create_group(self, group_id, count=0):
        result = self.__sql_execute__(
            '''INSERT INTO groups (group_id, count)
              VALUES (?, ?)''',
            (
                group_id,
                count
            )
        )

        return True if type(result) == list else False

    def group_increase(self, group_id):
        result = self.__sql_execute__(
            "UPDATE groups SET count = count + 1 WHERE group_id = ?",
            (
                group_id,
            )
        )

        return True if type(result) == list else False

    def user_increase(self, user_id):
        result = self.__sql_execute__(
            "UPDATE users SET count = count + 1 WHERE user_id = ?",
            (
                user_id,
            )
        )

        return True if type(result) == list else False

    def create_connection(self, group_id, user_id):
        result_query = self.__sql_execute__(
            '''INSERT INTO connection (group_id, user_id)
              VALUES (?, ?)''',
            (
                group_id,
                user_id
            )
        )

        result = True if type(result_query) == list else False

        if result is False:
            return False

        result_query_group = self.group_increase(group_id)
        result_query_user = self.user_increase(user_id)

        return result == result_query_group == result_query_user
