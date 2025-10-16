from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from backend.core.config import get_settings

# SQLAlchemy 2.0 style Base
class Base(DeclarativeBase):
    pass

settings = get_settings()

# Async engine & session factory
engine = create_async_engine(settings.DATABASE_URL, future=True, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Dependency for FastAPI (async generator)
async def get_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session
