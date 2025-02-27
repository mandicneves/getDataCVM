from setuptools import setup, find_packages

setup(
    name="getDataCVM",
    version="0.1.0",
    author="Mandic Neves",
    author_email="mandicneves@gmail.com",
    description="A package for downloading and processing CVM data.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/mandicneves/getDataCVM",
    packages=find_packages(),
    install_requires=["requests", "pandas", "beautifulsoup4"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
