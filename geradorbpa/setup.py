from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="geradorbpa",
    version="0.1.0",
    author="André",
    author_email="seu.email@exemplo.com",
    description="Gerador de Boletins de Produção Ambulatorial (BPA)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/seu-usuario/geradorbpa",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Healthcare Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    install_requires=[
        "flask==2.3.3",
        "pandas==1.5.3",
        "flask_sqlalchemy==3.0.5",
        "psycopg2==2.9.9",
        "python-dotenv==1.0.0",
        "pydantic==2.3.0",
        "pydantic-settings==2.0.3",
        "flask-restx==1.1.0",
    ],
    extras_require={
        "dev": [
            "pytest==7.4.2",
            "pytest-cov==4.1.0",
            "black==23.7.0",
            "flake8==6.1.0",
        ],
    },
    python_requires=">=3.8",
) 