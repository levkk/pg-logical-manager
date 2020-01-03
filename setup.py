import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='pg-logical-manager',
    version='0.4.2',
    author='Lev Kokotov',
    author_email='lev.kokotov@instacart.com',
    description="View and manage logical subscriptions for a PostgreSQL cluster.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/levkk/pg-logical-manager',
    install_requires=[
        'Click>=7.0',
        'colorama>=0.4.3',
        'prettytable>=0.7.2',
        'psycopg2>=2.8.4',
        'python-dotenv>=0.10.3',
    ],
    extras_require={
        'dev': 'pytest'
    },
    packages=setuptools.find_packages(exclude=('tests',)),
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent', # Colorama!
    ],
    python_requires='>=3.6', # f strings
    entry_points={
        'console_scripts': [
            'pglogicalmanager = pglogicalmanager:main',
        ]
    },
)