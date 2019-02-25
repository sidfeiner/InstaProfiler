from setuptools import setup, find_packages

version_info = (0, 0, 2)
version = '.'.join(map(str, version_info))

setup(
    name='insta-profiler',
    version=version,
    license='For internal usage only',
    author='Sidney Feiner.',
    description='Scrapers for instagram',
    packages=find_packages(include='InstaProfiler'),
    install_requires=[
        'pyodbc>=4.0.16',
        'fire>=0.1.1',
        'requests>=2.12.1',
        'selenium',
        'pandas'
    ]
)
