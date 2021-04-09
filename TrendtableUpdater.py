from . import mysql
from . import TerminalReporter
from . import trendtable
from . import SQLTemples

import os
import json


class Updater:

    def __init__(self, sql_config_path: str = None):
        if sql_config_path == None:
            self.sql_config_path = "./sql_config.json"
        else:
            self.sql_config_path = sql_config_path
        self.__get_sql_config()
        self.symbolsDB = mysql.DB("symbols", self.host,
                                  self.port, self.user, self.password)
        self.__save_sql_config()
        self.symbolsTB = self.symbolsDB.TB("symbols")
        self.historicalPriceDB = mysql.DB(
            "historical_price", self.host, self.port, self.user, self.password)
        self.trendtableDB = mysql.DB(
            "trendtable", self.host, self.port, self.user, self.password)

    def update_US(self):
        """
        Update trendtable of US symbols.
        """

        reporter = TerminalReporter.Reporter(
            "TrendtableUpdater", "Updateing US symbols...")
        reporter.report()

        symbols = self.symbolsTB.query(
            "*", "WHERE market = 'US' AND enable = true")

        reporter.initialize_stepIntro(len(symbols))
        for symbol in symbols:

            reporter.nowStep += 1
            reporter.what = "Updateing {}...".format(symbol)

            tbName = symbol.lower()
            price_tb = self.historicalPriceDB.TB(tbName)
            if not tbName in self.trendtableDB.list_tb():
                self.__create_tb_with_templates(
                    self.trendtableDB, tbName, SQLTemples.TRENDTABLE)
            trend_tb = self.trendtableDB.TB(tbName)

            # inital check
            price = price_tb.query("date, close")
            price_dates = list(price.keys())
            # we need at least 378 days record for calculate trendtable
            # so we skip data less than 378 records
            if len(price_dates) < 378:
                continue
            trend_dates = trend_tb.query("date")
            # we get the date of last update of trendtable
            # we only update the records with date bigger than last_date
            if len(trend_dates) == 0:
                last_date = -14831769600000
            else:
                last_date = max(list(trend_dates.keys()))
            updates = {}  # update data container
            # get the price
            price = price_tb.query("date, close")
            saveCount = 0 # counting for partly save
            reporter.initialize_substepIntro(len(price_dates))
            for i in range(len(price_dates)):
                reporter.nowSubstep += 1
                date = price_dates[i]
                if i < 378:  # at least 378 data points for trendtable calculatio7n
                    continue
                if date < last_date:  # skip date has been updated
                    continue
                reporter.report(True, True)
                series = []
                for i2 in range(i-378+2, i+1):
                    date2 = price_dates[i2]
                    series.append(price[date2]["close"])
                trendVals = trendtable.cal_trend_series(series, 3)
                updates[date] = trendVals
                # Since error raises if too many records are updated in one sql quote,
                # we break down the updates.
                saveCount += 1
                if saveCount == 1000: # save per 1000 records
                    trend_tb.update(updates)
                    self.trendtableDB.commit()
                    updates = {}
                    saveCount = 0
            # update to sql server
            trend_tb.update(updates)
            self.trendtableDB.commit()
        reporter.what = "Done."
        reporter.report()

    def __create_tb_with_templates(self, DB: object, tableName: str, temp: dict):
        colnames = list(temp.keys())
        # first column as key column
        tb = DB.add_tb(tableName, colnames[0], temp[colnames[0]])
        for i in range(1, len(colnames)):
            colname = colnames[i]
            tb.add_col(colname, temp[colname])

    def __get_sql_config(self):
        if not os.path.exists(self.sql_config_path):
            with open(self.sql_config_path, 'w') as f:
                j = {"host": "", "port": 0, "user": "", "password": ""}
                f.write(json.dumps(j))
        with open(self.sql_config_path, 'r') as f:
            sql_config = json.loads(f.read())
        self.host = sql_config["host"]
        self.port = sql_config["port"]
        self.user = sql_config["user"]
        self.password = sql_config["password"]

    def __save_sql_config(self):
        j = self.symbolsDB.get_loginInfo()
        with open(self.sql_config_path, 'w') as f:
            f.write(json.dumps(j))
