"""
POSIZIONE: mdg-v0/auth/api/auth_main.py

MDG Auth API — FastAPI-Users + JWT
Ruoli supportati: admin, user
Schema Postgres dedicato: usr
"""

import os
import uuid
from enum import Enum
from typing import AsyncGenerator, Optional

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi_users import BaseUserManager, FastAPIUsers, UUIDIDMixin, schemas
from fastapi_users.authentication import AuthenticationBackend, BearerTransport, JWTStrategy
from fastapi_users.db import SQLAlchemyBaseUserTableUUID, SQLAlchemyUserDatabase
from sqlalchemy import Column, String, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATABASE_URL = (
    f"postgresql+asyncpg://"
    f"{os.getenv('POSTGRES_USER', 'mdg_user')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'changeme')}@"
    f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'mdg')}"
)

JWT_SECRET     = os.getenv("JWT_SECRET", "CHANGE_THIS_SECRET_IN_PRODUCTION")
JWT_LIFETIME_S = int(os.getenv("JWT_LIFETIME_SECONDS", str(60 * 60 * 8)))  # 8h

# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

engine        = create_async_engine(DATABASE_URL)
async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class RoleEnum(str, Enum):
    admin_role    = "admin_role"
    it_role       = "it_role"
    business_role = "business_role"


class User(SQLAlchemyBaseUserTableUUID, Base):
    """Tabella utenti nello schema dedicato 'usr'."""
    __tablename__ = "users"
    __table_args__ = {"schema": "usr"}

    role:      str = Column(String(20),  nullable=False, default=RoleEnum.business_role)
    full_name: str = Column(String(100), nullable=True)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as session:
        yield session


async def get_user_db(session: AsyncSession = Depends(get_async_session)):
    yield SQLAlchemyUserDatabase(session, User)


# ---------------------------------------------------------------------------
# Schemas Pydantic
# ---------------------------------------------------------------------------

class UserRead(schemas.BaseUser[uuid.UUID]):
    role:      str
    full_name: Optional[str] = None


class UserCreate(schemas.BaseUserCreate):
    role:      str = RoleEnum.business_role
    full_name: Optional[str] = None


class UserUpdate(schemas.BaseUserUpdate):
    role:      Optional[str] = None
    full_name: Optional[str] = None


# ---------------------------------------------------------------------------
# User Manager
# ---------------------------------------------------------------------------

class UserManager(UUIDIDMixin, BaseUserManager[User, uuid.UUID]):
    reset_password_token_secret   = JWT_SECRET
    verification_token_secret     = JWT_SECRET

    async def on_after_register(self, user: User, request: Optional[Request] = None):
        print(f"[AUTH] Registrazione: {user.email} | ruolo: {user.role}")

    async def on_after_login(self, user: User, request: Optional[Request] = None, response=None):
        print(f"[AUTH] Login: {user.email} | ruolo: {user.role}")


async def get_user_manager(user_db=Depends(get_user_db)):
    yield UserManager(user_db)


# ---------------------------------------------------------------------------
# JWT Strategy & Auth Backend
# ---------------------------------------------------------------------------

bearer_transport = BearerTransport(tokenUrl="auth/jwt/login")


def get_jwt_strategy() -> JWTStrategy:
    return JWTStrategy(secret=JWT_SECRET, lifetime_seconds=JWT_LIFETIME_S)


auth_backend = AuthenticationBackend(
    name="jwt",
    transport=bearer_transport,
    get_strategy=get_jwt_strategy,
)

fastapi_users = FastAPIUsers[User, uuid.UUID](get_user_manager, [auth_backend])

current_active_user = fastapi_users.current_user(active=True)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="MDG Auth API",
    description="Autenticazione e gestione utenti — schema Postgres: usr",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # restringi in produzione OCI
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    fastapi_users.get_auth_router(auth_backend),
    prefix="/auth/jwt",
    tags=["Auth"],
)
app.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    prefix="/auth",
    tags=["Auth"],
)
app.include_router(
    fastapi_users.get_users_router(UserRead, UserUpdate),
    prefix="/users",
    tags=["Utenti"],
)

# ---------------------------------------------------------------------------
# Endpoint aggiuntivi
# ---------------------------------------------------------------------------

@app.get("/me", tags=["Auth"])
async def get_me(user: User = Depends(current_active_user)):
    """Dati dell'utente corrente — usato da Streamlit per verificare il token."""
    return {
        "id":        str(user.id),
        "email":     user.email,
        "role":      user.role,
        "full_name": user.full_name,
        "is_active": user.is_active,
    }


@app.get("/health", tags=["Sistema"])
async def health():
    return {"status": "ok"}


@app.get("/admin/users", tags=["Utenti"])
async def list_users(
    session: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(current_active_user),
):
    """Lista tutti gli utenti — accessibile a admin_role e it_role, senza richiedere is_superuser."""
    from sqlalchemy import select as sa_select
    from fastapi import HTTPException
    if current_user.role not in ("admin_role", "it_role"):
        raise HTTPException(status_code=403, detail="Accesso negato.")
    result = await session.execute(sa_select(User).order_by(User.email))
    users = result.scalars().all()
    return [
        {
            "id":        str(u.id),
            "email":     u.email,
            "role":      u.role,
            "full_name": u.full_name,
            "is_active": u.is_active,
        }
        for u in users
    ]


@app.patch("/admin/users/{user_id}", tags=["Utenti"])
async def update_user_by_id(
    user_id: str,
    payload: dict,
    session: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(current_active_user),
):
    """Aggiorna is_active o role — accessibile a admin_role e it_role."""
    from sqlalchemy import select as sa_select
    from fastapi import HTTPException
    import uuid as uuid_mod
    if current_user.role not in ("admin_role", "it_role"):
        raise HTTPException(status_code=403, detail="Accesso negato.")
    result = await session.execute(
        sa_select(User).where(User.id == uuid_mod.UUID(user_id))
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Utente non trovato.")
    if "is_active" in payload:
        user.is_active = payload["is_active"]
    if "role" in payload:
        user.role = payload["role"]
    await session.commit()
    return {"id": str(user.id), "email": user.email, "role": user.role, "is_active": user.is_active}


@app.post("/admin/users/{user_id}/password", tags=["Utenti"])
async def reset_password(
    user_id: str,
    payload: dict,
    session: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(current_active_user),
):
    """Reset password di un utente — accessibile a admin_role e it_role."""
    from sqlalchemy import select as sa_select
    from fastapi import HTTPException
    import uuid as uuid_mod
    from fastapi_users.password import PasswordHelper

    if current_user.role not in ("admin_role", "it_role"):
        raise HTTPException(status_code=403, detail="Accesso negato.")

    new_password = payload.get("password", "").strip()
    if not new_password:
        raise HTTPException(status_code=422, detail="Password non può essere vuota.")

    result = await session.execute(
        sa_select(User).where(User.id == uuid_mod.UUID(user_id))
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Utente non trovato.")

    ph = PasswordHelper()
    user.hashed_password = ph.hash(new_password)
    await session.commit()
    return {"detail": "Password aggiornata."}


# ---------------------------------------------------------------------------
# Startup: crea schema usr e tabelle se non esistono
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS usr"))
        await conn.run_sync(Base.metadata.create_all)
    print("[AUTH] Schema 'usr' e tabella 'usr.users' pronti.")
