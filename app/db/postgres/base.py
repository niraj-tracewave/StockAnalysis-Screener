from typing import Optional, List, Dict, Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import DeclarativeBase, declared_attr, load_only

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import select
from fastapi import HTTPException

class Base(DeclarativeBase):
    @declared_attr.directive
    def __tablename__(cls) -> str:  # type: ignore[override]
        return cls.__name__.lower()

class ExternalBase(DeclarativeBase):
    @declared_attr.directive
    def __tablename__(cls) -> str:  # type: ignore[override]
        return cls.__name__.lower()

class BaseDBOperations:
    """
    Generic reusable class for all PostgreSQL DB operations.
    Works with async SQLAlchemy ORM.
    """

    def __init__(self, db_session, model):
        self.db = db_session
        self.model = model

    # CREATE
    async def create(self, data: dict):
        instance = self.model(**data)
        return await self._commit(instance)

    # SAVE (existing instance)
    async def save(self, instance):
        return await self._commit(instance)

    # INTERNAL COMMIT LOGIC
    async def _commit(self, instance):
        try:
            self.db.add(instance)
            await self.db.commit()
            await self.db.refresh(instance)
            return instance

        except SQLAlchemyError as e:
            await self.db.rollback()
            raise HTTPException(
                status_code=400,
                detail={"error": [str(e)]}
            )

    # RETRIEVE SINGLE ROW
    async def retrieve(self, **filters):
        stmt = select(self.model).filter_by(**filters)
        result = await self.db.execute(stmt)
        return result.scalars().first()

    async def load_only_fields(model, fields: list[str]):
        return load_only(*[getattr(model, f) for f in fields])

    # RETRIEVE SINGLE ROW with selected column
    async def retrieve_selected_columns(self, columns, **filters):
        stmt = select(self.model).options(load_only(*columns)).filter_by(**filters)
        result = await self.db.execute(stmt)
        return result.scalars().first()

    # LIST ALL (with optional filters)
    async def list(self, **filters):
        stmt = select(self.model)

        if filters:
            stmt = stmt.filter_by(**filters)

        result = await self.db.execute(stmt)
        return result.scalars().all()

    # UPDATE INSTANCE
    async def update(self, instance, data: dict):
        for key, value in data.items():
            setattr(instance, key, value)
        return await self._commit(instance)

    # DELETE INSTANCE
    async def delete(self, instance):
        try:
            await self.db.delete(instance)
            await self.db.commit()
            return True

        except SQLAlchemyError as e:
            await self.db.rollback()
            raise HTTPException(
                status_code=400,
                detail={"error": [str(e)]}
            )

    async def bulk_insert(
            self,
            data:  List[Dict[str, Any]],
            conflict_fields: Optional[List[str]] = None
    ) -> int:
        """
        Bulk insert records.
        If conflict_fields provided -> ON CONFLICT DO NOTHING

        Returns: number of inserted rows
        """

        if not data:
            return 0

        try:
            stmt = insert(self.model).values(data)

            if conflict_fields:
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=conflict_fields
                )

            result = await self.db.execute(stmt)
            await self.db.commit()

            return result.rowcount or 0

        except SQLAlchemyError as e:
            await self.db.rollback()
            raise HTTPException(
                status_code=400,
                detail={"error": [str(e)]}
            )
