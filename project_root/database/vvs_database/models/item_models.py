from sqlalchemy import (
    Column, 
    Integer, 
    String, 
    Float,
    Boolean,
    ForeignKey, 
    UniqueConstraint,
    DateTime,
    Index,
    select,
    exists,
    and_,
    delete,
    func
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import relationship

from vvs_database.core import Base
from vvs_database.models.job_models.hc_models import HCInputJob, HCResult

class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, index=True)
    item = Column(String, unique=True, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # @classmethod
    # async def cleanup_unreferenced(cls, session: AsyncSession):
    #     """
    #     Delete items that aren't referenced in item_sources, item_results, 
    #     assemblies (as products), or assembly_components (as components) tables.
    #     Returns number of items deleted.
    #     """
    #     delete_stmt = delete(cls).where(
    #         and_(
    #             ~exists().where(ItemSource.item_id == cls.id),
    #             ~exists().where(ItemResult.item_id == cls.id),
    #             ~exists().where(Assembly.product_id == cls.id),
    #             ~exists().where(AssemblyComponent.component_id == cls.id),
    #         )
    #     ).returning(cls.id)

    #     result = await session.execute(delete_stmt)
    #     deleted_rows = result.scalars().all()
        
    #     await session.commit()
        
    #     return len(deleted_rows)
    
    @classmethod
    async def cleanup_unreferenced(cls, session: AsyncSession):
        """
        Delete items that aren't referenced in item_sources, item_results, 
        assemblies (as products), assembly_components (as components), or
        the new hill climbing tables.
        Returns number of items deleted.
        """
        delete_stmt = delete(cls).where(
            and_(
                ~exists().where(ItemSource.item_id == cls.id),
                ~exists().where(ItemResult.item_id == cls.id),
                ~exists().where(Assembly.product_id == cls.id),
                ~exists().where(AssemblyComponent.component_id == cls.id),
                ~exists().where(HCInputJob.item_id == cls.id),
                ~exists().where(HCResult.item_id == cls.id)
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

class ItemResult(Base):
    __tablename__ = "item_results"

    item_id = Column(Integer, ForeignKey("items.id", ondelete="CASCADE"), primary_key=True)
    plugin_id = Column(Integer, ForeignKey("plugins.id", ondelete="CASCADE"), primary_key=True)
    valid = Column(Boolean, nullable=False)
    score = Column(Float, nullable=True)
    embedding = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    item = relationship("Item", passive_deletes=True)
    plugin = relationship("Plugin", passive_deletes=True)

    __table_args__ = (
        UniqueConstraint('item_id', 'plugin_id', name='uix_item_result'),
    )

class Assembly(Base):
    __tablename__ = "assemblies"

    assembly_id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("items.id", ondelete="CASCADE"), nullable=False, index=True)
    plugin_id = Column(Integer, ForeignKey("plugins.id", ondelete="CASCADE"), nullable=False, index=True)
    assembly_key = Column(String, nullable=False, index=True)
    component_key = Column(String, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    product = relationship("Item", passive_deletes=True)
    plugin = relationship("Plugin", passive_deletes=True)
    components = relationship("AssemblyComponent", 
                            back_populates="assembly",
                            cascade="all, delete-orphan",
                            passive_deletes=True)

    __table_args__ = (
        Index('idx_assembly_product_plugin', 'product_id', 'plugin_id'),
        UniqueConstraint('assembly_key', name='uix_assembly_key'),
    )

    @property
    def generate_assembly_key(self):
        """Generate assembly key from components and product"""
        component_ids = [c.component_id for c in sorted(self.components, key=lambda x: x.assembly_index)]
        return f"{self.plugin_id}_{'_'.join(map(str, component_ids))}_{self.product_id}"
    
    @property
    def generate_component_key(self):
        """Generate component key from components and product"""
        component_ids = [c.component_id for c in sorted(self.components, key=lambda x: x.assembly_index)]
        return f"{self.plugin_id}_{'_'.join(map(str, component_ids))}"

    @classmethod
    async def get_or_create(cls, 
                            session: AsyncSession, 
                            product_id: int, 
                            plugin_id: int, 
                            component_ids: list[int],
                            ):
        """Get existing assembly or create new one"""
        # Create temporary instance to generate key
        temp_assembly = cls(product_id=product_id, plugin_id=plugin_id)
        temp_assembly.components = [
            AssemblyComponent(assembly_index=idx, component_id=comp_id)
            for idx, comp_id in enumerate(component_ids)
        ]
        assembly_key = temp_assembly.generate_assembly_key
        component_key = temp_assembly.generate_component_key
        
        # Check if assembly exists
        stmt = select(cls).where(cls.assembly_key == assembly_key)
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        
        if existing:
            return existing
        
        # Create new assembly
        temp_assembly.assembly_key = assembly_key
        temp_assembly.component_key = component_key
        session.add(temp_assembly)
        await session.flush()  # Flush to get the assembly_id
        
        return temp_assembly


class AssemblyComponent(Base):
    __tablename__ = "assembly_components"

    assembly_id = Column(Integer, ForeignKey("assemblies.assembly_id", ondelete="CASCADE"), primary_key=True)
    assembly_index = Column(Integer, primary_key=True)
    component_id = Column(Integer, ForeignKey("items.id", ondelete="CASCADE"), nullable=False, index=True)
    
    assembly = relationship("Assembly", back_populates="components", passive_deletes=True)
    component = relationship("Item", passive_deletes=True)

    __table_args__ = (
        UniqueConstraint('assembly_id', 'assembly_index', name='uix_assembly_component'),
    )


