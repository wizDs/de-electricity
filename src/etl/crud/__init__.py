from toolz.itertoolz import partition_all
from sqlalchemy.future.engine import Engine
from sqlmodel import SQLModel, Session, create_engine, select
from typing import Iterable, TypeVar
from tqdm import tqdm

# SQLModelType = TypeVar('SQLModel', bound=SQLModel)

class CRUD:
    def __init__(self, engine: Engine, table: SQLModel):
        self.engine = engine
        self.table = table
    
    def create(self, rows: Iterable[SQLModel], batch_size: int=1_000) -> None:
        assert all(isinstance(row, self.table) for row in rows)

        for partition in tqdm(list(partition_all(batch_size, rows))):
            with Session(self.engine) as session:
                try:
                    for row in partition:
                        session.add(row)
                    session.commit()
                except:
                    session.rollback()

    def read(self, filters: list=None) -> list[SQLModel]:
        with Session(self.engine) as session:
            if filters:
                result = session.query(self.table).where(*filters).all()
            else:
                result = session.query(self.table).all()
        return result

    def update(self) -> None:
        raise NotImplementedError

    def delete(self, filters: list=None, batch_size: int=1_000) -> None:
        result = self.read(filters)

        for partition in tqdm(list(partition_all(batch_size, result))):
            with Session(self.engine) as session:
                try:
                    for row in partition:
                        session.delete(row)
                    session.commit()
                except Exception as e:
                    print(e)
                    session.rollback()

