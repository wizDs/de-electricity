from sqlalchemy.future.engine import Engine
from sqlmodel import SQLModel, Session, create_engine, select
from typing import Iterable, TypeVar

# SQLModelType = TypeVar('SQLModel', bound=SQLModel)

class CRUD:
    def __init__(self, engine: Engine, table: SQLModel):
        self.engine = engine
        self.table = table
    
    def create(self, rows: Iterable[SQLModel]) -> None:
        assert all(isinstance(row, self.table) for row in rows)

        with Session(self.engine) as session:
            for row in rows:
                try:
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

    def delete(self, filters: list=None) -> None:
        result = self.read(filters)
        with Session(self.engine) as session:
            for row in result:
                try:
                    session.delete(row)
                    session.commit()
                except Exception as e:
                    print(e)
                    session.rollback()

