# -*- coding: utf-8 -*-
# @Time: 2019/4/10
# @File: split_table_date

import datetime
import calendar


class GenerateDate(object):

    season_dict = {
        0: [1,2,3],
        1: [4,5,6],
        2: [7,8,9],
        3: [10,11,12]
    }

    def __init__(self, date):
        self.date = date
        self.result = []
        self.table_list = []

    def get_start_end_date(self):
        try:
            split_data = self.date.split("-")
            if len(split_data) == 1:
                self.real_start_time = datetime.datetime.strptime(split_data[0], "%Y%m%d")
                self.real_end_time = self.real_start_time + datetime.timedelta(days=1)
            else:
                self.real_start_time, self.real_end_time = datetime.datetime.strptime(split_data[0], "%Y%m%d"), datetime.datetime.strptime(split_data[1], "%Y%m%d")
            self.diff_day = (self.real_end_time - self.real_start_time).days
            self.real_start_month_num, self.real_start_year_num = self.real_start_time.month, self.real_start_time.year
            self.real_end_month_num, self.real_end_year_num = self.real_end_time.month, self.real_end_time.year
            # week
            self.start_week_time = self.real_start_time - datetime.timedelta(days=self.real_start_time.weekday()) - datetime.timedelta(weeks=1)
            self.end_week_time = self.real_end_time - datetime.timedelta(days=self.real_end_time.weekday()) - datetime.timedelta(weeks=1)
            # month
            if self.real_start_month_num == 1:
                self.start_month_time = self.real_start_time - datetime.timedelta(days=self.real_start_time.day - 1) - datetime.timedelta(days=calendar.monthrange(self.real_start_year_num - 1, 12)[1])
            else:
                self.start_month_time = self.real_start_time - datetime.timedelta(days=self.real_start_time.day - 1) - datetime.timedelta(days=calendar.monthrange(self.real_start_year_num, self.real_start_month_num - 1)[1])
            if self.real_end_month_num == 1:
                self.end_month_time = datetime.datetime.strptime("{}{}{}".format(self.real_start_year_num - 1, 12, calendar.monthrange(self.real_end_year_num - 1, 12)[1]), "%Y%m%d")
            else:
                self.end_month_time = self.real_end_time - datetime.timedelta(days=self.real_end_time.day)
            # season
            for season, months in self.season_dict.items():
                if self.real_start_month_num in months:
                    self.season_num = season
                    if season == 0:
                        self.start_season_time = datetime.datetime.strptime("{}{}{}".format(self.real_start_year_num - 1, self.season_dict[3][0], 1), "%Y%m%d")
                    else:
                        self.start_season_time = datetime.datetime.strptime("{}{}{}".format(self.real_start_year_num, self.season_dict[season - 1][0], 1), "%Y%m%d")
                if self.real_end_month_num in months:
                    if season == 0:
                        self.end_season_time = datetime.datetime.strptime("{}{}{}".format(self.real_end_year_num - 1, self.season_dict[3][-1], calendar.monthrange(self.real_end_year_num - 1, self.season_dict[3][-1])[1]), "%Y%m%d")
                    else:
                        self.end_season_time = datetime.datetime.strptime("{}{}{}".format(self.real_end_year_num, self.season_dict[season - 1][-1], calendar.monthrange(self.real_end_year_num, self.season_dict[season - 1][-1])[1]), "%Y%m%d")
            # year
            self.start_year_time = datetime.datetime.strptime("{}0101".format(self.real_start_year_num), "%Y%m%d")
            self.end_year_time = datetime.datetime.strptime("{}12{}".format(self.real_end_year_num, calendar.monthrange(self.real_end_year_num, 12)[1]), "%Y%m%d")
            # halfyear
            self.start_halfyear_time = datetime.datetime.strptime("{}0101".format(self.real_start_year_num), "%Y%m%d")
            self.end_halfyear_time = datetime.datetime.strptime("{}0701".format(self.real_start_year_num), "%Y%m%d")
            return True
        except Exception as err:
            data = "Increment Date Error: {}".format(str(err))
            self.result.append(data)
            return False

    def do_work(self):
        self.get_start_end_date()
        self.do_week()
        self.do_month()
        self.do_season()
        self.do_year()
        self.do_halfyear()
        self.do_all()

    def change_filter(self, s_time, e_time, filter):
        write_table_name = "cache_{}_{}".format(filter, s_time.strftime("%Y%m%d"))
        self.table_list.append(write_table_name)

    def do_week(self):
        s = self.start_week_time
        while True:
            e = s + datetime.timedelta(weeks=1)
            if not self.change_filter(s, e, "week"):
                return False
            s = e
            if s >= self.end_week_time:
                break
        return True

    def do_month(self):
        s = self.start_month_time
        while True:
            e = s + datetime.timedelta(days=calendar.monthrange(s.year, s.month)[1])
            if not self.change_filter(s, e, "month"):
                return False
            s = e
            if s >= self.end_month_time:
                break
        return True

    def do_season(self):
        s = self.start_season_time
        while True:
            for season, months in self.season_dict.items():
                if s.month in months:
                    e = datetime.datetime.strptime("{}{}{}".format(s.year, months[-1], calendar.monthrange(s.year, months[-1])[1]), "%Y%m%d") + datetime.timedelta(days=1)
                    if not self.change_filter(s, e, "season"):
                        return False
                    s = e
                    break
            if s >= self.end_season_time:
                break
        return True

    def do_halfyear(self):
        s = self.start_halfyear_time
        while True:
            if s.month > 6:
                e = datetime.datetime.strptime("{}12{}".format(s.year, calendar.monthrange(s.year, 12)[1]), "%Y%m%d") + datetime.timedelta(days=1)
            else:
                e = datetime.datetime.strptime("{}6{}".format(s.year, calendar.monthrange(s.year, 6)[1]), "%Y%m%d") + datetime.timedelta(days=1)
            if not self.change_filter(s, e, "halfyear"):
                return False
            s = e
            if s >= self.end_halfyear_time:
                break
        return True

    def do_year(self):
        s = self.start_year_time
        while True:
            e = datetime.datetime.strptime("{}12{}".format(s.year, calendar.monthrange(s.year, 12)[1]), "%Y%m%d") + datetime.timedelta(days=1)
            if not self.change_filter(s, e, "year"):
                return False
            s = e
            if s >= self.end_year_time:
                break
        return True

    def do_all(self):
        write_table_name = "cache_{}_{}".format("all", 19700101)
        self.table_list.append(write_table_name)
