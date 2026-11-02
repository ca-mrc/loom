"""Management-owned policy for one shared physical cache scope."""
from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Text, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class NebiusCacheRuntimePolicy(Base):
    __tablename__ = "nebius_cache_runtime_policies"
    __table_args__ = (
        CheckConstraint("cache_total_mib BETWEEN 1024 AND 1048576", name="nebius_cache_runtime_policy_budget_check"),
        CheckConstraint("cache_gc_policy IN ('legacy-seven-day','budget-pressure')", name="nebius_cache_runtime_policy_gc_check"),
    )
    storage_endpoint: Mapped[str] = mapped_column(Text, primary_key=True)
    storage_region: Mapped[str] = mapped_column(Text, primary_key=True)
    cache_bucket: Mapped[str] = mapped_column(Text, primary_key=True)
    installation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    cache_total_mib: Mapped[int] = mapped_column(BigInteger, nullable=False)
    cache_gc_policy: Mapped[str] = mapped_column(Text, nullable=False)
    reuse_task_cache_for_verifier: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
