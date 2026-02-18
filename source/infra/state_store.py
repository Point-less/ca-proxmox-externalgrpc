from __future__ import annotations

import time
from pathlib import Path

from sqlalchemy import Integer, String, Text, and_, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import text

from core.contracts import VMStateRecord


class Base(DeclarativeBase):
    pass


class VmStateRow(Base):
    __tablename__ = "vm_states"

    vmid: Mapped[int] = mapped_column(Integer, primary_key=True)
    group_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    vm_name: Mapped[str] = mapped_column(String(255), nullable=False)
    state: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    pending_since: Mapped[int | None] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[int] = mapped_column(Integer, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    cleanup_storage: Mapped[str | None] = mapped_column(String(255), nullable=True)
    cleanup_volume: Mapped[str | None] = mapped_column(Text, nullable=True)


class GroupSizeRow(Base):
    __tablename__ = "group_sizes"

    group_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    desired_size: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[int] = mapped_column(Integer, nullable=False)


class StateStore:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._engine = create_async_engine(f"sqlite+aiosqlite:///{self.db_path}", future=True)
        self._sessions = async_sessionmaker(self._engine, expire_on_commit=False, class_=AsyncSession)
        self._initialized = False

    def _to_record(self, row: VmStateRow) -> VMStateRecord:
        return VMStateRecord(
            vmid=int(row.vmid),
            group_id=str(row.group_id),
            vm_name=str(row.vm_name),
            state=str(row.state),
            pending_since=(int(row.pending_since) if row.pending_since is not None else None),
            updated_at=int(row.updated_at),
            last_error=(str(row.last_error) if row.last_error is not None else None),
            cleanup_storage=(str(row.cleanup_storage) if row.cleanup_storage is not None else None),
            cleanup_volume=(str(row.cleanup_volume) if row.cleanup_volume is not None else None),
        )

    async def init(self) -> None:
        if self._initialized:
            return
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            cols = {
                str(row[1])
                for row in (await conn.execute(text("PRAGMA table_info(vm_states)"))).fetchall()
                if len(row) >= 2
            }
            if "cleanup_storage" not in cols:
                await conn.execute(text("ALTER TABLE vm_states ADD COLUMN cleanup_storage VARCHAR(255)"))
            if "cleanup_volume" not in cols:
                await conn.execute(text("ALTER TABLE vm_states ADD COLUMN cleanup_volume TEXT"))
        self._initialized = True

    async def upsert_vm_state(
        self,
        *,
        vmid: int,
        group_id: str,
        vm_name: str,
        state: str,
        pending_since: int | None,
        last_error: str | None = None,
        cleanup_storage: str | None = None,
        cleanup_volume: str | None = None,
    ) -> None:
        now = int(time.time())
        async with self._sessions() as session, session.begin():
            stmt = sqlite_insert(VmStateRow).values(
                vmid=int(vmid),
                group_id=str(group_id),
                vm_name=str(vm_name),
                state=str(state),
                pending_since=pending_since,
                updated_at=now,
                last_error=last_error,
                cleanup_storage=cleanup_storage,
                cleanup_volume=cleanup_volume,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[VmStateRow.vmid],
                set_={
                    "group_id": str(group_id),
                    "vm_name": str(vm_name),
                    "state": str(state),
                    "pending_since": pending_since,
                    "updated_at": now,
                    "last_error": last_error,
                    "cleanup_storage": cleanup_storage,
                    "cleanup_volume": cleanup_volume,
                },
            )
            await session.execute(stmt)

    async def get_vm_state(self, vmid: int) -> VMStateRecord | None:
        async with self._sessions() as session:
            row = await session.get(VmStateRow, int(vmid))
            if row is None:
                return None
            return self._to_record(row)

    async def list_group_vm_states(self, group_id: str) -> list[VMStateRecord]:
        async with self._sessions() as session:
            rows = (
                await session.execute(select(VmStateRow).where(VmStateRow.group_id == str(group_id)).order_by(VmStateRow.vmid))
            ).scalars()
            return [self._to_record(row) for row in rows]

    async def delete_vm_state(self, vmid: int) -> None:
        async with self._sessions() as session, session.begin():
            row = await session.get(VmStateRow, int(vmid))
            if row is not None:
                await session.delete(row)

    async def count_group_vm_states(self, group_id: str, states: set[str]) -> int:
        if not states:
            return 0
        async with self._sessions() as session:
            result = await session.execute(
                select(func.count())
                .select_from(VmStateRow)
                .where(and_(VmStateRow.group_id == str(group_id), VmStateRow.state.in_(list(states))))
            )
            return int(result.scalar_one() or 0)

    async def get_desired_size(self, group_id: str) -> int | None:
        async with self._sessions() as session:
            row = await session.get(GroupSizeRow, str(group_id))
            if row is None:
                return None
            return int(row.desired_size)

    async def set_desired_size_if_missing(self, group_id: str, desired_size: int) -> None:
        now = int(time.time())
        async with self._sessions() as session, session.begin():
            stmt = sqlite_insert(GroupSizeRow).values(
                group_id=str(group_id), desired_size=int(desired_size), updated_at=now
            )
            stmt = stmt.on_conflict_do_nothing(index_elements=[GroupSizeRow.group_id])
            await session.execute(stmt)

    async def set_desired_size(self, group_id: str, desired_size: int) -> None:
        now = int(time.time())
        async with self._sessions() as session, session.begin():
            stmt = sqlite_insert(GroupSizeRow).values(
                group_id=str(group_id), desired_size=int(desired_size), updated_at=now
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[GroupSizeRow.group_id],
                set_={"desired_size": int(desired_size), "updated_at": now},
            )
            await session.execute(stmt)
