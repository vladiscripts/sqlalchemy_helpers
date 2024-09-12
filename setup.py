from setuptools import setup, find_packages


def readme():
    with open('README.md', 'r') as f:
        return f.read()


setup(
    name='sqlalchemy_helpers',
    version='1.0.0',
    author='vladiscripts',
    author_email='blagopoluchie12@gmail.com',
    description='Some helpers for sSQLAlchemy',
    long_description=readme(),
    long_description_content_type='text/markdown',
    url='https://github.com/vladiscripts/sqlalchemy_helpers',
    packages=find_packages(),
    install_requires=['sqlalchemy'],
    classifiers=[
        'Programming Language :: Python :: 3.10',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent'
    ],
    keywords='sqlalchemy',
    project_urls={
        'Documentation': 'https://github.com/vladiscripts/sqlalchemy_helpers'
    },
    python_requires='>=3.10'
)
