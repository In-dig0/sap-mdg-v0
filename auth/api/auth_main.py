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
from sqlalchemy import Boolean, Column, String, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from sqlalchemy import select as sa_select
from fastapi_users.password import PasswordHelper

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

    role:                 str  = Column(String(20),  nullable=False, default=RoleEnum.business_role)
    full_name:            str  = Column(String(100), nullable=True)
    must_change_password: bool = Column(Boolean,     nullable=False, default=False)


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
        "id":                   str(user.id),
        "email":                user.email,
        "role":                 user.role,
        "full_name":            user.full_name,
        "is_active":            user.is_active,
        "must_change_password": user.must_change_password,
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
            "id":                   str(u.id),
            "email":                u.email,
            "role":                 u.role,
            "full_name":            u.full_name,
            "is_active":            u.is_active,
            "must_change_password": u.must_change_password,
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

    # it_role non può modificare utenti admin_role
    if current_user.role == "it_role" and user.role == "admin_role":
        raise HTTPException(status_code=403, detail="Non puoi modificare un utente con ruolo admin_role.")

    # it_role non può assegnare il ruolo admin_role
    if current_user.role == "it_role" and payload.get("role") == "admin_role":
        raise HTTPException(status_code=403, detail="Non puoi assegnare il ruolo admin_role.")

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
    user.must_change_password = False
    await session.commit()
    return {"detail": "Password aggiornata."}


@app.patch("/admin/users/{user_id}/force-password-change", tags=["Utenti"])
async def force_password_change(
    user_id: str,
    payload: dict,
    session: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(current_active_user),
):
    """Imposta o azzera il flag must_change_password per un utente."""
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
    user.must_change_password = payload.get("must_change_password", True)
    await session.commit()
    return {"detail": "Flag aggiornato.", "must_change_password": user.must_change_password}


# ---------------------------------------------------------------------------
# Startup: crea schema usr e tabelle se non esistono
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Variabili seed admin (dal docker-compose via .env)
# ---------------------------------------------------------------------------
ADMIN_EMAIL    = os.getenv("ADMIN_EMAIL")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
ADMIN_NAME     = os.getenv("ADMIN_NAME", "Amministratore MDG")


@app.on_event("startup")
async def on_startup():
    # 1. Crea schema e tabelle
    async with engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS usr"))
        await conn.run_sync(Base.metadata.create_all)
    print("[AUTH] Schema 'usr' e tabella 'usr.users' pronti.")

    # 2. Seed admin automatico
    if not ADMIN_EMAIL or not ADMIN_PASSWORD:
        print("[AUTH] ⚠️  ADMIN_EMAIL/ADMIN_PASSWORD non impostati — seed saltato.")
        return



    async with async_session() as session:
        result = await session.execute(
            sa_select(User).where(User.email == ADMIN_EMAIL)
        )
        if result.scalar_one_or_none() is None:
            ph = PasswordHelper()
            admin = User(
                id=uuid.uuid4(),
                email=ADMIN_EMAIL,
                hashed_password=ph.hash(ADMIN_PASSWORD),
                role=RoleEnum.admin_role,
                full_name=ADMIN_NAME,
                is_active=True,
                is_superuser=True,
                is_verified=True,
                must_change_password=False,
            )
            session.add(admin)
            await session.commit()
            print(f"[AUTH] ✅ Admin '{ADMIN_EMAIL}' creato automaticamente.")
        else:
            print(f"[AUTH] Admin '{ADMIN_EMAIL}' già presente — seed saltato.")
