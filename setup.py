import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='pg-logical-manager', # Replace with your own username
    version='0.2',
    author='Lev Kokotov',
    author_email='lev.kokotov@gmail.com',
    description="View and manage logical subscriptions for a PostgreSQL cluster.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/levkk/pg-logical-manager',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)