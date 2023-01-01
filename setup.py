import os

from setuptools import find_packages, setup


ROOT = os.path.abspath(os.path.dirname(__file__))


with open(os.path.join(ROOT, "README.md")) as f:
    long_description = f.read()

with open(os.path.join(ROOT, "requirements.txt")) as f:
    install_requires = f.read().splitlines()


setup(
    name="TyDB",
    description="A simple, type-friendly ORM.",
    long_description=long_description,
    python_requires=">=3.7",
    install_requires=install_requires,
    license="BSD 3-Clause License",
    platforms=["Any"],
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries",
        "Typing :: Typed",
    ],
)
