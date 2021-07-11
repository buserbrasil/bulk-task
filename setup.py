from setuptools import find_packages, setup

setup(
    packages=find_packages(),
    include_package_data=True,
    setup_requires=[
        'pytest-runner',
    ],
    tests_require=[
        'fakeredis',
        'pydantic',
        'pytest',
        'pytest-cov',
        'pytest-flake8',
    ],
)
