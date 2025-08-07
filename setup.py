"""
GFK ETL Library 安装脚本
"""

from setuptools import setup, find_packages

# 读取README文件
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# 读取requirements文件
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="gfk-pcr-tyre-etl",
    version="2.0.0",
    author="Julian Luan",
    author_email="julian.luan@sailun-tyres.eu",
    description="GFK PCR轮胎市场数据ETL处理库 - 2025年CSV数据专用",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/julianluan/gfk-pcr-tyre-etl",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Office/Business",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "gfk-pcr-etl=main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "gfk_etl_library": [
            "config/*.yml",
            "docs/*.md",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/julianluan/gfk-pcr-tyre-etl/issues",
        "Source": "https://github.com/julianluan/gfk-pcr-tyre-etl",
        "Documentation": "https://github.com/julianluan/gfk-pcr-tyre-etl/blob/main/docs/",
    },
)