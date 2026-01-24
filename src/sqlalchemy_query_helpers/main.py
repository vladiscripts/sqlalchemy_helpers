from sqlalchemy import create_engine, MetaData, Table, select, update
from sqlalchemy.orm import sessionmaker, Query, DeclarativeMeta, session
from sqlalchemy.orm import declarative_base
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

    def __init__(self, db_name, base: DeclarativeMeta, db_url: str | None = None, echo=False, create_tables=False):
        """
        :param db_url: URL as "{user}:{password}@{host}" without [schema + netloc], 
            that will use as f'mysql+pymysql://{db_url}'. 
            It's convenient to store this string as an OS environment variable.
            None - Use OS environment variables: 'DB_USER', 'DB_PASSWORD', 'DB_HOST'.

            Доступ к БД по логину:паролю через localhost:3306 блокирутся по сети. Необходимо использовать ssh.
            В консоли надо запустить переброску порта на localhost:3307:
                ssh -L 3307:127.0.0.1:3306 root@remote_ip -N
            И строку подключения заменить на: "user:password@localhost:3307"
                db_url='user:password@127.0.0.1:3307'
        """
        self.base = base
        engine_str = self.make_engine_str(db_url)
        self.engine = create_engine(f'{engine_str}/{db_name}', echo=echo)
        self.Session = sessionmaker(bind=self.engine)
        # self.session = self.Session()

        if create_tables:
            base.metadata.create_all(self.engine)  # create tables and index if not exists
        self.name = self.engine.url.database

    def __del__(self):
        if self.session:
            self.session.close()

    @staticmethod
    def make_engine_str(db_url: str | None) -> str:
        """
        Create an engine string (schema + netloc), like "mysql+pymysql://USER:PASSWORD@HOST"
        :param db_url: URL as "{user}:{password}@{host}" that will use as f'mysql+pymysql://{db_url}'. 
            It's convenient to store this string as an OS environment variable.
            None - Use OS environment variables: 'DB_USER', 'DB_PASSWORD', 'DB_HOST'.
        """
        if not db_url:
            import os
            try:
                user = os.environ['DB_USER']
                password = os.environ['DB_PASSWORD']
                host = os.environ['DB_HOST']
            except KeyError:
                raise RuntimeError("Set the 'DB_USER', 'DB_PASSWORD', 'DB_HOST' OS env variables")
            engine_str = f'mysql+pymysql://{user}:{password}@{host}'
        else:
            engine_str = f'mysql+pymysql://{db_url}'
        return engine_str

    def get_predefined_table(self, table_name: str, base_metadata=None) -> Table:
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
        dicts = [self.__to_dict(row, mfields, use_orm_keys=True) for row in rows]
        if not dicts:
            return

        with self.Session() as session:
            if hasattr(t, '_sa_class_manager'):  # ORM-модель
                session.bulk_insert_mappings(t, dicts)
            else:  # Table
                stmt = insert(t).values(dicts)
                session.execute(stmt)
            if do_commit:
                session.commit()

    def insert_one(self, t, row: Union[dict, list, tuple], mfields: Union[list, tuple] = None, ignore=False):
        q = insert(t).values(self.__to_dict(row, mfields))
        if ignore:
            q = q.prefix_with('IGNORE', dialect='mysql')
        with self.Session() as s:
            r = s.execute(q)
            s.commit()
        return r.lastrowid

    def insert_ignore(self, t, row: Union[dict, list, tuple], mfields: Iterable[InstrumentedAttribute] = None) -> bool:
        is_inserted = self.insert_ignore_many(t, [row], mfields)
        return is_inserted

    def insert_ignore_many(self, t, rows: List[dict], mfields: Iterable[InstrumentedAttribute] = None) -> bool:
        with self.Session() as s:
            is_inserted = False
            for row in rows:
                row = self.__to_dict(row, mfields, use_orm_keys=True)
                try:
                    with s.begin_nested():
                        m = t(**row)
                        s.add(m)
                    is_inserted = True
                except IntegrityError:
                    # print(f'already in DB: {row}')
                    pass
            s.commit()
        return is_inserted

    def insert_ignore_core(self, t, row: Union[dict, list, tuple], mfields: Union[list, tuple] = None) -> None:
        """Core instead ORM. IGNORE can ignore don't only doubles. Many warnings."""
        self.insert_ignore_many_core(t, [row], mfields)

    def insert_ignore_many_core(self, t, rows: List[Union[dict, list, tuple]], mfields: Union[list, tuple] = None) -> None:
        """If can better use upsert, or insert after select with filtering exists rows. Problems of IGNORE: 
        * This make very large skips of row ids in table.
        * Can ignore don't only doubles but other errors. Many warnings."""
        dicts = [self.__to_dict(row, mfields) for row in rows]  # , use_orm_keys=True
        if not dicts:
            return
        q = insert(t).values(dicts).prefix_with('IGNORE', dialect='mysql')
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
        with self.Session() as s:
            rows_updates = s.query(t).filter_by(**in_keys).update(not_in_keys)
        # q = update(t).values(**not_in_keys).where(**in_keys)
        # rows_updates = self.db.session.execute(q)
        # self.db.session.commit()
        return rows_updates

    def upsert_many_with_flags(self, t, rows: List[Union[dict, list, tuple]], cause_keys: Union[list, tuple],
                               mfields: Union[list, tuple] = None) -> dict:
        """
        Пакетный upsert с возвратом флагов.
        :param t: ORM-модель таблицы
        :param rows: Список данных (dict или list)
        :param cause_keys: Ключи, по которым проверяется наличие (например, [Data.Date, 'ProductID'])
        :param mfields: Поля модели, если `rows` — списки
        :return: {
            'inserted': [...],  # список вставленных row-объектов
            'updated': [...],   # список обновлённых row-объектов
            'skipped': [...]    # без изменений
        }
        """
        from decimal import Decimal
        import datetime as dt

        def _values_equal(a, b) -> bool:
            """
            Сравнивает два значения как 'равные' с точки зрения бизнес-логики и SQL.
            Важно: не использует float() для Decimal, чтобы избежать потерь.
            """
            if a is None and b is None:
                return True
            if a is None or b is None:
                return False

            # Случай: int, float, Decimal — сравниваем как числа
            numeric_types = (int, float, Decimal)
            if isinstance(a, numeric_types) and isinstance(b, numeric_types):
                # Приводим к Decimal для точного сравнения
                try:
                    dec_a = Decimal(a) if not isinstance(a, Decimal) else a
                    dec_b = Decimal(b) if not isinstance(b, Decimal) else b
                    return dec_a == dec_b
                except (InvalidOperation, TypeError):
                    return False  # если не удалось привести

            # Случай: дата/время
            datetime_types = (dt.date, dt.datetime, dt.time)
            if isinstance(a, datetime_types) and isinstance(b, datetime_types):
                return a == b

            # Все остальное — строгое сравнение
            return a == b

        dicts = [self.__to_dict(row, mfields, use_orm_keys=True) for row in rows]

        is_orm_model = hasattr(t, '_sa_class_manager')
        is_table = isinstance(t, Table)
        if not (is_orm_model or is_table):
            raise TypeError("t must be ORM class or sqlalchemy.Table")

        # Обработка cause_keys
        key_columns = []
        key_names = []
        for key in cause_keys:
            if isinstance(key, str):
                col = getattr(t, key) if is_orm_model else t.c[key]
                key_columns.append(col)
                key_names.append(key)
            elif hasattr(key, 'key'):
                key_columns.append(key)
                key_names.append(key.key)
            else:
                raise TypeError(f"Unsupported cause_keys element: {type(key)}")

        def make_key(row):
            if isinstance(row, dict):
                return tuple(row[k] for k in key_names)
            else:
                return tuple(getattr(row, k) for k in key_names)

        update_candidates = [(row, make_key(row)) for row in dicts]

        # === 1. Массовый SELECT ===
        existing = {}
        keys_to_find = [key for _, key in update_candidates]
        if keys_to_find:
            filters = []
            for i, col in enumerate(key_columns):
                values = [key[i] for key in keys_to_find]
                filters.append(col.in_(values))

            with self.Session() as session:
                if is_orm_model:
                    query = session.query(t).filter(*filters).all()
                else:
                    stmt = select(t).filter(*filters)
                    query = session.execute(stmt).fetchall()
                    query = [row._asdict() for row in query]

                for row in query:
                    key = make_key(row)
                    existing[key] = row

        # === 2. Сравнение ===
        inserted = []
        updated = []
        skipped = []

        for row_dict, key in update_candidates:
            if key in existing:
                db_row = existing[key]
                do_update = False
                for k, v in row_dict.items():
                    if k in key_names:
                        continue
                    db_val = getattr(db_row, k) if not isinstance(db_row, dict) else db_row[k]
                    if not _values_equal(db_val, v):
                        do_update = True
                        break
                if do_update:
                    updated.append(row_dict)
                else:
                    skipped.append(row_dict)
            else:
                inserted.append(row_dict)

        # === 3. Пакетная вставка/обновление ===
        if inserted or updated:
            combined = inserted + updated
            stmt = insert(t).values(combined)

            if is_orm_model:
                cols = t.__table__.columns
            else:
                cols = t.columns
            upsert_cols = [c.name for c in cols if not (c.primary_key or c.unique)]
            update_dict = {col: getattr(stmt.inserted, col) for col in upsert_cols if col not in key_names}

            if update_dict:
                upsert_query = stmt.on_duplicate_key_update(update_dict)
                with self.Session() as session:
                    session.execute(upsert_query)
                    session.commit()

        return {'inserted': inserted, 'updated': updated, 'skipped': skipped}

    def update_with_select(self, t, row: Union[dict, list, tuple], cause_dict: Union[list, tuple], mfields: Union[list, tuple] = None) -> (bool,
                                                                                                                                           bool):
        """
        Проверяет, существует ли запись. Если да — обновляет, только если значения отличаются.
        Возвращает: (is_exists, is_updated)
        """
        row = self.__to_dict(row, mfields, use_orm_keys=True)
        cause_dict, to_insert_dict = self.__check_modelkeys(row, cause_dict)
        is_updated = is_exists = False

        with self.Session() as session:
            r = session.query(t).filter_by(**cause_dict).first()
            if not r:
                return is_updated, is_exists

            is_exists = True
            do_update = False
            for k, v in to_insert_dict.items():
                if getattr(r, k) != v:
                    setattr(r, k, v)
                    do_update = True

            if do_update:
                session.commit()
                is_updated = True

            return is_updated, is_exists

    def upsert_with_select(self, t, row: Union[dict, list, tuple], cause_keys: Union[list, tuple], mfields: Union[list, tuple] = None) -> (bool,
                                                                                                                                           bool):
        """
        Упрощённая версия, использующая upsert_many_with_flags.
        """
        result = self.upsert_many_with_flags(t, [row], cause_keys, mfields)
        inserted = len(result['inserted']) > 0
        updated = len(result['updated']) > 0
        return updated, inserted

    def upsert(self, t, rows: Union[list[dict], tuple[dict]], mfields=None, do_commit=True, filter_unque_primary_keys=True):
        dicts = [self.__to_dict(row, mfields) for row in rows]
        if not dicts:
            return

        stmt = insert(t).values(dicts)
        # need to remove primary or unique keys on using, else will error
        if filter_unque_primary_keys:
            table_columns = t.columns._all_columns if isinstance(t, Table) else t._sa_class_manager.mapper.columns._all_columns
            update_dict = {x.name: x for x in stmt.inserted for c in table_columns
                           if x.name == c.name and c.unique is not True and c.primary_key is not True}
        else:
            update_dict = {x.name: x for x in stmt.inserted}
        if not update_dict:
            return
        upsert_query = stmt.on_duplicate_key_update(update_dict)
        with self.Session() as session:
            session.execute(upsert_query)
            if do_commit:
                session.commit()

    def execute_sqls(self, sqls: Union[str, list, tuple]):
        assert isinstance(sqls, (str, list, tuple))
        if isinstance(sqls, str):
            sqls = [sqls]
        with self.engine.connect() as conn:
            for s in sqls:
                conn.execute(sql.text(s))
