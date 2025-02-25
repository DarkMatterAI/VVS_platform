from sqlalchemy import (
    Column, 
    Integer, 
    String, 
    Float,
    ForeignKey, 
    UniqueConstraint,
    DateTime,
    Index,
    select,
    exists,
    or_,
    not_,
    and_,
    delete,
    func
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import relationship

from vvs_database.core import Base

class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, index=True)
    item = Column(String, unique=True, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    @classmethod
    async def cleanup_unreferenced(cls, session: AsyncSession):
        """
        Delete items that aren't referenced in item_sources, item_scores, 
        assemblies (as products), or assembly_components (as components) tables.
        Returns number of items deleted.
        """
        delete_stmt = delete(cls).where(
            and_(
                ~exists().where(ItemSource.item_id == cls.id),
                ~exists().where(ItemScore.item_id == cls.id),
                # ~exists().where(Assembly.product_id == cls.id),
                # ~exists().where(AssemblyComponent.component_id == cls.id)
            )
        ).returning(cls.id)

        result = await session.execute(delete_stmt)
        deleted_rows = result.scalars().all()
        
        await session.commit()
        
        return len(deleted_rows)


class ItemSource(Base):
    __tablename__ = "item_sources"

    item_id = Column(Integer, ForeignKey("items.id", ondelete="CASCADE"), primary_key=True)
    external_id = Column(String, nullable=True)
    plugin_id = Column(Integer, ForeignKey("plugins.id", ondelete="CASCADE"), primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    item = relationship("Item", passive_deletes=True)
    source_plugin = relationship("Plugin", passive_deletes=True)

    __table_args__ = (
        UniqueConstraint('item_id', 'plugin_id', name='uix_item_source'),
    )

class ItemScore(Base):
    __tablename__ = "item_scores"

    item_id = Column(Integer, ForeignKey("items.id", ondelete="CASCADE"), primary_key=True)
    plugin_id = Column(Integer, ForeignKey("plugins.id", ondelete="CASCADE"), primary_key=True)
    score = Column(Float, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    item = relationship("Item", passive_deletes=True)
    plugin = relationship("Plugin", passive_deletes=True)

    __table_args__ = (
        UniqueConstraint('item_id', 'plugin_id', name='uix_item_score'),
    )