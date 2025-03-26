from setuptools import setup

setup(
    name="getDataCVM",
    version="1.0.0",
    author=" Marcelo Neves Lira",
    author_email="mandicneves@gmail.com",
    description="A package for downloading and processing CVM data.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    platforms="any",
    packages=["getDataCVM"],
    install_requires=["requests", "pandas", "beautifulsoup4"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    url="https://github.com/mandicneves/getDataCVM",
)
