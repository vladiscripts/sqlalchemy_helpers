from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.orm import sessionmaker, Query, DeclarativeMeta, session
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy import sql
from typing import Iterable, Union, List, Sequence


class DB:
    is_updated = False
    name: str
    base: DeclarativeMeta = None
    engine = None
    Session: sessionmaker = None
    session: Session = None

    def __init__(self, db_name, base: DeclarativeMeta, use_os_env=True, echo=False):
        self.base = base
        engine_str = self.make_engine_str(use_os_env)
        self.engine = create_engine(f'{engine_str}/{db_name}', echo=echo)
        self.Session = sessionmaker(bind=self.engine)
        # self.session = self.Session()

        base.metadata.create_all(self.engine)  # create tables and index if not exists
        self.name = self.engine.url.database

    def __del__(self):
        if self.session:
            self.session.close()

    @staticmethod
    def make_engine_str(use_os_env) -> str:
        """
        Create an engine string (schema + netloc), like "mysql+pymysql://USER:PASSWORD@HOST"
        :param use_os_env: Use OS envs 'DB_USER', 'DB_PASSWORD', 'DB_HOST', instead the `cfg.py` file
        """
        if use_os_env:
            import os
            try:
                user = os.environ['DB_USER']
                password = os.environ['DB_PASSWORD']
                host = os.environ['DB_HOST']
            except KeyError:
                raise RuntimeError("Set the 'DB_USER', 'DB_PASSWORD', 'DB_HOST' OS env variables")
        else:
            from cfg import user, password, host
        engine_str = f'mysql+pymysql://{user}:{password}@{host}'
        return engine_str

    def get_predefined_table(self, table_name: str, base_metadata=None):
        table = Table(table_name, base_metadata or declarative_base().metadata, autoload_with=self.engine)
        return table

    @staticmethod
    def __check_modelkeys(row: dict, cause_dict: Iterable[InstrumentedAttribute]) -> (dict, dict):
        """
        :param row: from self.to_dict()
        :param cause_dict: the model keys with values to search in database
        :return:
            cause_dict: the model keys with values to search in database
            to_insert_dict: names of database's columns with new values to change
        """
        model_keys = [n.key for n in cause_dict]
        cause_dict = {k: v for k, v in row.items() if k in model_keys}
        to_insert_dict = {k: v for k, v in row.items() if k not in model_keys}
        return cause_dict, to_insert_dict

    def __to_dict(self, row: Union[dict, list], mfields: Sequence[InstrumentedAttribute] = None,
                  use_mfield_keys=True, use_orm_keys=False) -> dict:
        """ Convert to dict.

        :param row: List values or dict with column name/values. Column names can be stings or model columns.
        :param mfields: List of fields of table's model. As sqlalchemy Column, not strings.
        :param use_mfield_keys: Leave mfields as model fields, without converting it to strings.
        """
        if isinstance(row, dict):
            if [k for k, v in row.items() if isinstance(k, InstrumentedAttribute)]:
                d = {k.key: v for k, v in row.items()} if use_orm_keys else {k.name: v for k, v in row.items()}
            else:
                d = row
            d = self.clean_values(d)
            return d
        elif mfields:
            assert isinstance(row, (list, tuple, str))
            fields = [f.key for f in mfields] if use_mfield_keys else [f.name for f in mfields]
            if isinstance(row, (list, tuple)):
                assert len(mfields) == len(row), "len(mfields) != len(row)"
            elif isinstance(row, str):
                assert len(mfields) == 1, "len(mfields) != len(row)"
                row = [row]
            d = dict(zip(fields, row))
            d = self.clean_values(d)
            return d
        raise RuntimeError("unknown type 'row'")

    def clean_values(self, d: dict):
        """ strip() for str values"""
        d_new = {k: v.strip() or None if isinstance(v, str) else v for k, v in d.items()}
        return d_new

    def insert(self, t, row: Union[dict, list, tuple], mfields: Union[list, tuple] = None, do_commit=True):
        """  todo: new_rowid is None. Function 'insert_many' doesn't return anything. But it can be used in some old scripts."""
        new_rowid = self.insert_many(t, [row], mfields, do_commit)
        return new_rowid

    def insert_many(self, t, rows: Union[list, tuple], mfields: Union[list, tuple] = None, do_commit=True):
        for row in rows:
            row = self.__to_dict(row, mfields, use_orm_keys=True)
            m = t(**row)
            self.session.add(m)
        if do_commit:
            self.session.commit()

    def insert_one(self, t, row: Union[list, tuple], mfields: Union[list, tuple] = None, ignore=False):
        q = insert(t).values(self.__to_dict(row, mfields))
        if ignore:
            q = q.prefix_with('IGNORE', dialect='mysql')
        r = self.session.execute(q)
        self.session.commit()
        return r.lastrowid

    def insert_ignore(self, t, row: Union[dict, list, tuple], mfields: Iterable[InstrumentedAttribute] = None) -> bool:
        is_inserted = self.insert_ignore_many(t, [row], mfields)
        return is_inserted

    def insert_ignore_many(self, t, rows: List[dict], mfields: Iterable[InstrumentedAttribute] = None) -> bool:
        is_inserted = False
        for row in rows:
            row = self.__to_dict(row, mfields, use_orm_keys=True)
            try:
                with self.session.begin_nested():
                    m = t(**row)
                    self.session.add(m)
                is_inserted = True
            except IntegrityError:
                # print(f'already in DB: {row}')
                pass
        self.session.commit()
        return is_inserted

    def insert_ignore_core(self, t, row: Union[dict, list, tuple], mfields: Union[list, tuple] = None) -> None:
        """Core instead ORM. IGNORE can ignore don't only doubles. Many warnings."""
        self.insert_ignore_many_core(t, [row], mfields)

    def insert_ignore_many_core(self, t, rows: List[Union[dict, list, tuple]], mfields: Union[list, tuple] = None) -> None:
        """If can better use upsert, or insert after select with filtering exists rows. Problems of IGNORE: 
        * This make very large skips of row ids in table.
        * Can ignore don't only doubles but other errors. Many warnings."""
        rows_to_insert = [self.__to_dict(row, mfields) for row in rows]
        q = insert(t).values(rows_to_insert).prefix_with('IGNORE', dialect='mysql')
        with self.Session() as session:
            session.execute(q)
            session.commit()

    def insert_ignore_instanses(self, instances):
        if not isinstance(instances, Iterable): instances = (instances,)
        for m in instances:
            try:
                with self.session.begin_nested():
                    self.session.add(m)
                    self.session.flush()
                    # print(f'DB: added {m}')
            except IntegrityError:
                pass
                # print(f'DB: already in {m}')
        # self.session.commit()

    def update(self, t, row: Union[dict, list, tuple], cause_keys: Union[list, tuple], mfields: Union[list, tuple] = None) -> (bool, bool):
        row = self.__to_dict(row, mfields)
        in_keys, not_in_keys = self.__check_modelkeys(row, cause_keys)  # get_check_args(row, keys)
        rows_updates = Query(t, session=self.session).filter_by(**in_keys).update(not_in_keys)
        # q = update(t).values(**not_in_keys).where(**in_keys)
        # rows_updates = self.db.session.execute(q)
        # self.db.session.commit()
        return rows_updates

    def update_with_select(self, t, row: Union[dict, list, tuple], cause_dict: Union[list, tuple], mfields: Union[list, tuple] = None) -> (
            bool, bool):
        """it dont works"""
        row = self.__to_dict(row, mfields)
        is_updated = exist = False
        cause_dict, to_insert_dict = self.__check_modelkeys(row, cause_dict)
        # r = Query(t, session=self.session).filter_by(**cause_dict).first()
        q = select(t).where(**cause_dict).limit(1)
        r = self.session.execute(q).first()
        if r:
            for k, v in to_insert_dict.items():
                if vars(r)[k] != v:
                    vars(r)[k] = v  # dont works
                    is_updated = True
            exist = True
        return exist, is_updated

    def upsert_with_select(self, t, row: Union[dict, list, tuple], cause_keys: Union[list, tuple],
                           mfields: Union[list, tuple] = None) -> (bool, bool):
        """ A use example

        with dbhandle.atomic():
            t = ProductsData
            for date, fid, value in data:
                is_updated, is_inserted = upsert(
                    t,
                    [date.date(), pid, flow_ids[fid], value],
                    mfields=[t.Date, t.ProductID, t.FlowID, t.Value],
                    cause_keys=[t.Date, t.ProductID, t.FlowID])


        :param t: table model
        :param row: data, dict, or given mfields then list
        :param cause_keys: model keys for cause in update. Uniques keys for rows.
        :param mfields: model keys, if row is list instead dict
        :return: tuple(bool(is_updated), bool(is_inserted))
        """
        row = self.__to_dict(row, mfields)
        is_updated = is_inserted = False
        self.session.begin_nested()
        exist, is_updated = self.update_with_select(t, row, cause_keys)
        if not exist and not is_updated:
            c = self.insert(t, row)
            # if c and c > 0:
            #     is_inserted = True
            is_inserted = True
        self.session.commit()
        return is_updated, is_inserted

    def upsert(self, t, rows: Union[list[dict], tuple[dict]], mfields=None, do_commit=True, filter_unque_primary_keys=True):
        rows_to_insert = [self.__to_dict(row, mfields) for row in rows]
        stmt = insert(t).values(rows_to_insert)
        # need to remove primary or unique keys on using, else will error
        if filter_unque_primary_keys:
            update_dict = {x.name: x for x in stmt.inserted for c in t._sa_class_manager.mapper.columns._all_columns
                           if x.name == c.name and c.unique is not True and c.primary_key is not True}
        else:
            update_dict = {x.name: x for x in stmt.inserted}
        if not update_dict:
            return
        upsert_query = stmt.on_duplicate_key_update(update_dict)
        with self.Session() as session:
            session.execute(upsert_query)
            if do_commit:
                try:
                    session.commit()
                except Exception as e:
                    session.rollback()

    # def upsert(self, t, row, mfields=None):
    #     row = self.to_dict(row, mfields)
    #     stmt = insert(t).values(row).on_duplicate_key_update(row)
    #     self.session.execute(stmt)
    #     # self.session.commit()

    # def __upsert_with_get_all_in_db(self, t, rows: List[Union[dict, tuple]], cause_keys: list, mfields: list = None):
    #     # all_in_db = self.session.query(t).all()
    #     # in_keys, not_in_keys = self.get_check_modelkeys(row, cause_keys)  # get_check_args(row, keys)
    #     all_in_db = self.session.query(*cause_keys).all()
    #     while rows:
    #         month, pid, fid, cid, v = rows.pop()
    #         for row in all_in_db:
    #             row = self.to_dict(row, mfields)
    #             in_keys, not_in_keys = self.get_check_modelkeys(row, cause_keys)
    #             if r.Month == month and r.id == pid and r.FlowID == fid and r.CountryID == cid:
    #                 r.Value = v
    #                 break
    #         else:
    #             m = t(month, pid, fid, cid, v)
    #             self.session.add(m)
    #     self.session.commit()

    # def _upsert_with_get_all_in_db(self, t, rows: List[tuple], cause_keys, ):
    #     in_keys, not_in_keys = self.get_check_modelkeys(row[], cause_keys)
    #     row = dict(zip([f.key for f in cause_keys], row))
    #
    #     all_in_db = self.session.query(t).all()
    #     while rows:
    #         month, pid, fid, cid, v = rows.pop()
    #         for r in all_in_db:
    #             if r.Month == month and r.id == pid and r.FlowID == fid and r.CountryID == cid:
    #                 r.Value = v
    #                 break
    #         else:
    #             m = t(month, pid, fid, cid, v)
    #             self.session.add(m)
    #     self.session.commit()

    def execute_sqls(self, sqls: Union[str, list, tuple]):
        assert isinstance(sqls, (str, list, tuple))
        if isinstance(sqls, str):
            sqls = [sqls]
        conn = self.engine.connect()
        for s in sqls:
            conn.execute(sql.text(s))
