import time
import vk
import logbook
import progressbar
import DbManager


class Analyzer(object):

    def __init__(self, access_token, database_name="vkAnalyzer.db", log_file="./output_analyzer.log", log_level=logbook.INFO):

        logbook.FileHandler(log_file, level=log_level).push_application()
        self.logger = logbook.Logger("VKAnalyzer")

        # Настройка прогресс бара
        self.bar = progressbar.ProgressBar()

        # Настройка базы данных
        self.db = DbManager.SQLiteDb(database_name=database_name)

        # Настройка API Вконтакте
        vk_session = vk.Session(access_token=access_token)
        self.vk = vk.API(vk_session, timeout=30)

    def get_count_members_group(self, group_id):
        self.logger.info("get count members in group #{0}", group_id)

        self.logger.trace("request vk.groups.getById(group_id={0},fields=\"members_count\")", group_id)
        group_info_request = self.vk.groups.getById(
            group_id=group_id,
            fields="members_count"
        )

        group_info = group_info_request[0]
        self.logger.debug("response groups.getById(groups_id={0}): {1}", group_id, group_info)

        if "deactivated" in group_info:
            self.logger.info("group #{0} is deactivated or not exists", group_id)
            return 0

        members_count = group_info["members_count"]
        self.logger.info("group #{0} has {1} users", group_id, members_count)

        self.logger.trace("get_count_members_group return: {0}", members_count)
        return members_count

    def user_groups(self, user_id):
        self.logger.info("get user group #{0}", user_id)

        offset = 0
        offset_step = 1000
        count_groups = 1

        while offset < count_groups:
            time.sleep(0.3)
            self.logger.trace("request vk.groups.get(user_id={})", user_id)
            groups = self.vk.groups.get(
                user_id=user_id,
                offset=offset
            )
            self.logger.trace("response groups.get(user_id={0}, offset={1}): {2}", user_id, offset, groups)

            # Выставление смещения и количества групп у пользователя
            offset += offset_step
            self.logger.trace("new offset={0} for groups user #{1}", offset, user_id)

            if count_groups == 1:
                count_groups = len(groups)
                self.logger.info("user #{0} has {1} groups", user_id, count_groups)

            for group_id in groups:
                self.logger.debug("check connection between user #{0} and group #{1}", user_id, group_id)
                if self.db.check_exist_connection(group_id, user_id) is False:
                    if self.db.check_exist_group(group_id) is False:
                        self.logger.info("Created new group #{0}", group_id)
                        self.db.create_group(group_id)
                    else:
                        self.logger.debug("group #{0} already exists", group_id)

                    result_create_connection = self.db.create_connection(group_id, user_id)
                    self.logger.info("{0} new connection between user #{1} and group #{2}",
                                     "create" if result_create_connection is True else "failed to create",
                                     user_id,
                                     group_id)
                else:
                    self.logger.debug("connection between user #{0} and group #{1} already exists.", user_id, group_id)

    def start_group(self, group_number, group_id):
        self.logger.info("group number: {0}, get members group #{1}", group_number, group_id)

        offset = 0
        offset_step = 1000
        count_members = 1
        current_member = 0

        while offset < count_members:
            self.logger.trace("request vk.groups.getMembers(group_id={0}, sort=\"id_asc\", offset={1}, count={2})",
                              group_id,
                              offset,
                              offset_step)

            members = self.vk.groups.getMembers(
                group_id=group_id,
                sort="id_asc",
                offset=offset,
                count=offset_step
            )
            self.logger.trace("response vk.groups.getMembers(group_id={0}, sort=\"id_asc\", offset={1}, count={2}): {3}",
                              group_id,
                              offset,
                              offset_step,
                              members)

            # Выставление смещения и количества групп у пользователя
            offset += offset_step
            self.logger.trace("new offset={0} for members group #{1}", offset, group_id)

            if count_members == 1:
                count_members = members["count"]
                self.bar.max_value = count_members
                self.logger.info("group #{0} has {1} members", group_id, count_members)

            for member in members["users"]:
                self.logger.trace("request vk.users.get(user_id={0})", member)
                user = self.vk.users.get(
                    user_ids=member
                )
                self.logger.trace("response vk.users.get(user_id={0}): {1}", member, user)

                user = user[0]

                if "deactivated" in user:
                    self.logger.info("user #{0} is deactivated or not exists", user["uid"])
                    continue

                user_id = user["uid"]
                user_first_name = user["first_name"]
                user_last_name = user["last_name"]

                if self.db.check_exist_user(user_id) is False:
                    self.logger.info("new user #{0}", user_id)
                    self.db.create_user(user_id, user_first_name, user_last_name)
                else:
                    self.logger.debug("user #{0} already exists", user_id)

                with self.logger.catch_exceptions():
                    self.bar.update(current_member)
                    self.user_groups(user_id)
                    current_member += 1


analyzer = Analyzer(
    access_token="34c6a9a14cc1b81c9169ee9b7d6046452e9be2166f62595adadc8b48d5cbb35dc3057af532f966ec67b87",
    log_level=logbook.ERROR
)

list_groups = {
    'amvnews'
}

index = 0
for group in list_groups:
    analyzer.start_group(index, group)
    index += 1
