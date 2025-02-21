from sqlalchemy import Column, Integer, String, ForeignKey, Float, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, index=True)
    item = Column(String, unique=True, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    @classmethod
    def cleanup_unreferenced(cls, session):
        """
        Delete items that aren't referenced in any other table.
        Returns number of items deleted.
        """
        subquery = select(1).where(
            or_(
                exists().where(ItemSource.item_id == cls.id),
                exists().where(ItemScore.item_id == cls.id),
                exists().where(Assembly.product_id == cls.id),
                exists().where(AssemblyComponent.component_id == cls.id)
            )
        ).correlate(cls.__table__)

        stmt = select(cls).where(not_(exists(subquery)))
        unreferenced = session.execute(stmt).scalars().all()
        
        for item in unreferenced:
            session.delete(item)
        
        session.commit()
        return len(unreferenced)

class ItemSource(Base):
    __tablename__ = "item_sources"

    item_id = Column(Integer, ForeignKey("items.id", ondelete="CASCADE"), primary_key=True)
    external_id = Column(String, nullable=True)
    source_plugin_id = Column(Integer, ForeignKey("plugins.id", ondelete="CASCADE"), primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    item = relationship("Item", passive_deletes=True)
    source_plugin = relationship("Plugin", passive_deletes=True)

    __table_args__ = (
        UniqueConstraint('item_id', 'source_plugin_id', name='uix_item_source'),
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

class Assembly(Base):
    __tablename__ = "assemblies"

    assembly_id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("items.id", ondelete="CASCADE"), nullable=False, index=True)
    plugin_id = Column(Integer, ForeignKey("plugins.id", ondelete="CASCADE"), nullable=False, index=True)
    assembly_key = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    product = relationship("Item", passive_deletes=True)
    plugin = relationship("Plugin", passive_deletes=True)
    components = relationship("AssemblyComponent", 
                            back_populates="assembly",
                            cascade="all, delete-orphan",
                            passive_deletes=True)

    __table_args__ = (
        Index('idx_assembly_product_plugin', 'product_id', 'plugin_id'),
    )

    @property
    def generate_assembly_key(self):
        """Generate assembly key from components and product"""
        component_ids = [c.component_id for c in sorted(self.components, key=lambda x: x.assembly_index)]
        return f"{self.plugin_id}_{'_'.join(map(str, component_ids))}_{self.product_id}"

    @classmethod
    def get_or_create(cls, session, product_id, plugin_id, component_ids):
        """Get existing assembly or create new one"""
        # Create temporary instance to generate key
        temp_assembly = cls(product_id=product_id, plugin_id=plugin_id)
        temp_assembly.components = [
            AssemblyComponent(assembly_index=idx, component_id=comp_id)
            for idx, comp_id in enumerate(component_ids)
        ]
        assembly_key = temp_assembly.generate_assembly_key()
        
        existing = session.query(cls).filter_by(assembly_key=assembly_key).first()
        if existing:
            return existing
        
        temp_assembly.assembly_key = assembly_key
        session.add(temp_assembly)
        return temp_assembly


class AssemblyComponent(Base):
    __tablename__ = "assembly_components"

    assembly_id = Column(Integer, ForeignKey("assemblies.assembly_id", ondelete="CASCADE"), primary_key=True)
    assembly_index = Column(Integer, primary_key=True)
    component_id = Column(Integer, ForeignKey("items.id", ondelete="CASCADE"), nullable=False)
    
    assembly = relationship("Assembly", back_populates="components", passive_deletes=True)
    component = relationship("Item", passive_deletes=True)

    __table_args__ = (
        UniqueConstraint('assembly_id', 'assembly_index', name='uix_assembly_component'),
    )



async def execute_plugin(db_plugin, execute_request):
    ... # code here omitted 
    response = await execution_function(db_plugin, execute_request)
    
    return response 



from typing import Dict, List, Union
from sqlalchemy.orm import Session

async def execute_plugin(db: Session, db_plugin: Plugin, execute_request: Union[Dict, List[Dict]]):
    # Handle both single and batch requests
    is_batch = isinstance(execute_request, list)
    requests = execute_request if is_batch else [execute_request]
    
    responses = await execution_function(db_plugin, execute_request)
    responses = responses if is_batch else [responses]
    
    # Process responses based on plugin type
    processed_responses = []
    
    for request, response in zip(requests, responses):
        if not response.get('valid', False):
            processed_responses.append(response)
            continue

        if db_plugin.type == PluginType.DATA_SOURCE:
            processed_response = process_data_source_response(db, db_plugin, response)
        elif db_plugin.type == PluginType.SCORE:
            processed_response = process_score_response(db, db_plugin, request, response)
        elif db_plugin.type == PluginType.ASSEMBLY:
            processed_response = process_assembly_response(db, db_plugin, request, response)
        else:
            processed_response = response
            
        processed_responses.append(processed_response)
    
    return processed_responses if is_batch else processed_responses[0]

def process_data_source_response(db: Session, db_plugin: Plugin, response: Dict) -> Dict:
    for result in response.get('result', []):
        # Get or create Item
        item = db.query(Item).filter_by(item=result['item']).first()
        if not item:
            item = Item(item=result['item'])
            db.add(item)
            db.flush()  # Get the ID
            
        # Get or create ItemSource
        item_source = db.query(ItemSource).filter_by(
            item_id=item.id,
            source_plugin_id=db_plugin.id
        ).first()
        
        if not item_source:
            item_source = ItemSource(
                item_id=item.id,
                external_id=result.get('external_id'),
                source_plugin_id=db_plugin.id
            )
            db.add(item_source)
    
    db.commit()
    return response

def process_score_response(db: Session, db_plugin: Plugin, request: Dict, response: Dict) -> Dict:
    if not response.get('valid', False):
        return response
        
    # Get or create Item
    item = db.query(Item).filter_by(item=request['item']).first()
    if not item:
        item = Item(item=request['item'])
        db.add(item)
        db.flush()
    
    # Update or create ItemScore
    item_score = db.query(ItemScore).filter_by(
        item_id=item.id,
        plugin_id=db_plugin.id
    ).first()
    
    if item_score:
        item_score.score = response['score']
    else:
        item_score = ItemScore(
            item_id=item.id,
            plugin_id=db_plugin.id,
            score=response['score']
        )
        db.add(item_score)
    
    db.commit()
    return response

def process_assembly_response(db: Session, db_plugin: Plugin, request: Dict, response: Dict) -> Dict:
    if not response.get('valid', False):
        return response
        
    parent_items = []
    for parent in request['parents']:
        item = db.query(Item).filter_by(item=parent['item']).first()
        if not item:
            item = Item(item=parent['item'])
            db.add(item)
            db.flush()
        parent_items.append((parent['assembly_index'], item))
    
    for result in response.get('result', []):
        # Get or create product Item
        product = db.query(Item).filter_by(item=result['item']).first()
        if not product:
            product = Item(item=result['item'])
            db.add(product)
            db.flush()
            
        # Create ItemSource if external_id exists
        if result.get('external_id'):
            item_source = ItemSource(
                item_id=product.id,
                external_id=result['external_id'],
                source_plugin_id=db_plugin.id
            )
            db.add(item_source)
        
        # Create Assembly record
        parent_ids = [parent[1].id for parent in sorted(parent_items, key=lambda x: x[0])]
        assembly = Assembly.get_or_create(
            db,
            product_id=product.id,
            plugin_id=db_plugin.id,
            component_ids=parent_ids
        )
        
        # If assembly already existed, skip creating components
        if not assembly.components:
            for idx, parent in parent_items:
                component = AssemblyComponent(
                    assembly_id=assembly.assembly_id,
                    assembly_index=idx,
                    component_id=parent.id
                )
                db.add(component)
    
    db.commit()
    return response













POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB_TEST = os.getenv('POSTGRES_DB_TEST')

SQLALCHEMY_DATABASE_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgresql/{POSTGRES_DB_TEST}"

# Connection URL to default postgres database
DEFAULT_DB_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgresql/postgres"
# Connection URL to test database
TEST_DB_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgresql/{POSTGRES_DB_TEST}"

@pytest.fixture(scope="session")
async def test_db():
    # First connect to default database to create test database
    default_engine = create_async_engine(
        DEFAULT_DB_URL,
        isolation_level="AUTOCOMMIT"  # Needed to create/drop database
    )

    async with default_engine.connect() as conn:
        # Disconnect all active connections to the test database
        await conn.execute(text(f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{POSTGRES_DB_TEST}'
            AND pid <> pg_backend_pid();
        """))
        
        # Drop test database if it exists and create it fresh
        await conn.execute(text(f"DROP DATABASE IF EXISTS {POSTGRES_DB_TEST}"))
        await conn.execute(text(f"CREATE DATABASE {POSTGRES_DB_TEST}"))

    await default_engine.dispose()

    # Now connect to test database and set up tables
    test_engine = create_async_engine(TEST_DB_URL)
    TestingSessionLocal = sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    try:
        yield TestingSessionLocal
    finally:
        async with test_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        
        # Clean up by dropping the test database
        default_engine = create_async_engine(
            DEFAULT_DB_URL,
            isolation_level="AUTOCOMMIT"
        )
        async with default_engine.connect() as conn:
            await conn.execute(text(f"""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{POSTGRES_DB_TEST}'
                AND pid <> pg_backend_pid();
            """))
            await conn.execute(text(f"DROP DATABASE IF EXISTS {POSTGRES_DB_TEST}"))
        await default_engine.dispose()
        await test_engine.dispose()


# @pytest.fixture(scope="session")
# async def test_db():
#     engine = create_async_engine(SQLALCHEMY_DATABASE_URL)
#     TestingSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

#     # Create test database
#     async with engine.begin() as conn:
#         await conn.run_sync(Base.metadata.create_all)

#     try:
#         yield TestingSessionLocal
#     finally:
#         async with engine.begin() as conn:
#             await conn.run_sync(Base.metadata.drop_all)
#         await engine.dispose()

@pytest.fixture(scope="function")
async def db_session(test_db):
    async with test_db() as session:
        # Start a nested transaction
        trans = await session.begin_nested()
        try:
            yield session
        finally:
            await trans.rollback()
            # Roll back the outer transaction
            await session.rollback()