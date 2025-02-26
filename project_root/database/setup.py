from setuptools import setup, find_packages

setup(
    name="vvs_database",
    version="0.1.0",
    description="Database interface library for PostgreSQL",
    author="Your Team",
    # packages=find_packages(),
    packages=find_packages() + ['tests'],
    package_data={
        'tests': ['*.py'],
    },
    install_requires=[
        "pydantic",
        "pydantic-settings",
        "sqlalchemy[asyncio]",
        "asyncpg",
        "psycopg2-binary",
        "httpx",
        "aioredis",
        "pika"
    ],
    extras_require={
        "test": [
            "pytest",
            "pytest-asyncio",
        ],
    },
    python_requires=">=3.9",
)