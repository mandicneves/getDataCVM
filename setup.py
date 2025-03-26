from setuptools import setup, find_packages

setup(
    name="data_cvm",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A package for downloading and processing CVM data.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/data_cvm",
    packages=find_packages(),
    install_requires=["requests", "pandas", "beautifulsoup4"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
