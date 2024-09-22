Пакет содержит хелперы для пакета SQLAlchmy - методов `insert`, `upsert`, `update` и другое.

Инициализация: `db = DB(db_name, Base, use_os_env=False, echo=False)`. Где:
* `use_os_env`. Значение `True` означает брать определения для подключения к базе данных из переменных окружения хоста: `os.environ['DB_USER'], os.environ['DB_PASSWORD'], os.environ['DB_HOST']`. При значении `False` переменные user, password, host берутся из файла `cfg.py`, который должен быть создан в каталоге скрипта.

## Экземпляр класса содержит
Методы:
* `insert_many`, `upsert` и другие

Свойства:
* `engine` - подключение к базе данных
* `name` - имя базы данных
* `base`: DeclarativeMeta
* `Session: sessionmaker` - фабрика сессий
* `session: Session` - инициализированная сессия

## Пример использования

Определяем модель таблицы в файле `db_model.py`:
```python
from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.dialects.mysql import TINYINT, SMALLINT, INTEGER, ENUM, FLOAT
from sqlalchemy.schema import Index
from sqlalchemy.orm import declarative_base

Base = declarative_base()

db_name = 'some_database'


class ATable(Base):
    __tablename__ = 'A_table'
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
```

Основной файл:
```pythonа
from sqlalchemy_query_helpers import DB
from db_model import db_name, Base, ATable

db = DB(db_name, Base)

values_from_database = db.session.query(ATable).all()
```

#### Пример `cfg.py`
```python
# Settings of database
host = '100.100.100.100'
user = 'root'
password = 'qwerty'
```
