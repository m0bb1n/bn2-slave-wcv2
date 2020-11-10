import os

from bn2.drivers.slavedriver import SlaveDriver
import traceback
from bn2.db import db

from bn2.db import warehousedb

from bn2.utils.msgqueue \
    import create_local_task_message, INBOX_SYS_CRITICAL_MSG, INBOX_SYS_MSG, \
    INBOX_TASK1_MSG, PriorityQueue

from sqlalchemy import DateTime
import time

import requests
import json

import sys

from datetime import datetime

driver = SlaveDriver({})


class WarehouseClerkv2 (object):
    model_tag = "WCv2"

    def _init_WCv2_configs(self):
        driver.add_bot_config(
            'warehouse_db_engine',
            'Warehouse Database Engine',
            'str',
        )

        driver.add_bot_config(
            'warehouse_db_address',
            'Warehouse Database Address/File path',
            'str',
        )

        driver.load_bot_config_from_file()

    def __init__(self):
        self._init_WCv2_configs()
        driver.log.debug("Starting WarehouseClerk (WC)...")
        print(driver.get_bot_config("warehouse_db_engine"))
        self.warehouse_db = db.DBWrapper(
            driver.get_bot_config("warehouse_db_address"),
            warehousedb.Base,
            driver.get_bot_config("warehouse_db_engine"),
            create=True,
            scoped_thread=True
        )

    def get_model_from_table_name(self, table_name):
        model = getattr(warehousedb, table_name)
        if not model:
            raise ValueError("Table name {} is not in database".format(table_name))
        return model

    @driver.route("bd.sd.@WCv2.query")
    def bd_sd_WCv2_query(self, data, route_meta):
        query = data['query']
        tag = data.get('tag', None)
        if not tag:
            raise ValueError("Tag address is needed")


        with self.warehouse_db.scoped_session() as session:
            queried = self.warehouse_db.query_raw(query, session=session)
            driver.send_tag_data(tag, queried)

    def normalize_inputs(self, *, row=None, rows=[], model=None, table_name=None) -> list:
        if not model:
            if not table_name:
                raise ValueError("model or table_name has to be provided")

            model = self.get_model_from_table_name(table_name)

        if row:
            rows.append(row)

        for row in rows:
            for col in row:
                type_ = None
                try:
                    type_ = type(getattr(model, col).type)
                except:
                    pass
                else:
                    if type_ == DateTime:
                        row[col] = datetime.strptime(row[col], "%Y-%m-%d %H:%M:%S.%f")
        return rows

    @driver.route("bd.sd.@WCv2.model.insert")
    def bd_sd_WCv2_model_insert(self, data, route_meta):
        #Add insert options here like
        with self.warehouse_db.scoped_session() as session:
            model = self.get_model_from_table_name(data['table_name'])

            ids = []
            tag = data.get('tag', None)
            rows = self.normalize_inputs(rows=data['rows'], model=model)
            for row in rows:
                obj = session.scoped.add_model(model, row)
                if obj:
                    ids.append(obj.id)

            session.commit()

            if tag:
                driver.send_tag_data(tag, ids)


    @driver.route("bd.sd.@WCv2.model.remove")
    def bd_sd_WCv2_model_remove(self, data, route_meta):
        """
            {
                'table_name': 'test',
                'ids': [3, 12, 32]
            }

        """

        with self.warehouse_db.scoped_session() as session:
            table = data
            model = self.get_model_from_table_name(table['table_name'])
            for row_id in table['ids']:
                obj = session.query(model)\
                        .filter(row_id)\
                        .first()
                if obj:
                    session.scoped.delete(obj)
            session.commit()

    @driver.route("bd.sd.@WCv2.model.get")
    def bd_sd_WCv2_model_get(self, data, route_meta):
        model = self.get_model_from_table_name(data['table_name'])
        sid = data['sid']
        cnt = data.get('cnt')
        per = data.get('per')
        args = data.get('args')

        if not cnt:
            cnt = 0
        if not per or per > 30:
            per = 10

        _args = None
        if args:
            _args = [getattr(model, key)==value for key, value in args.items()]

        with self.warehouse_db.scoped_session() as session:
            query = session.query(model)

            if _args:
                query = query.filter(*_args)

            row_cnt = session.query(model).count()
            offset = cnt*per

            model_rows = query.offset(offset).limit(per).all()
            rows  = [self.warehouse_db.as_json(m) for m in model_rows]
            msg = driver.create_local_task_message(
                'bd.sd.@CPv2<warehouse.table',
                {"rows":rows, "sid":sid, "table_name":data["table_name"]}
            )
            driver.send_message_to(data['uuid'], msg)

    @driver.route("bd.sd.@WCv2.model.update")
    def bd_sd_WCv2_model_update(self, data, route_meta):
        """
            {
                'overwrite': True,
                'table_name': 'A_Table',
                'rows': [
                    {'id': 3, 'a_value': 8},
                    {'id': 7, 'b_value': 15}
                ]
            }
        """

        with self.warehouse_db.scoped_session() as session:
            table = data
            model = self.get_model_from_table_name(data['table_name'])
            overwrite = table["overwrite"]
            if not model:
                raise ValueError("Model '{}' does not exist".format(data['table_name']))

            rows = self.normalize_inputs(rows=data['rows'], model=model)

            for row in rows:
                row_id = row.pop('id', None)
                if not row_id:
                    continue

                obj = session.query(model).filter(model.id==row_id).first()
                if not obj:
                    continue

                for col in row:
                    if getattr(obj, col) != None and not overwrite:
                        continue
                    setattr(obj, col, row[col])

            session.commit()



wc = WarehouseClerkv2()
driver.register(wc)
driver.start()
