from setuptools import setup, find_packages

setup(
    name='PhonePe_Pulse_Visualization',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        # List your project dependencies here
        'requests',
        'mysql-connector-python',
        'pandas',
        'streamlit',
        'sqlalchemy',

    ],
    entry_points={
        'console_scripts': [
            # List any command-line scripts here
            'your_script_name=your_package.module:main',
        ],
    },
    author='Selvakumaran Devanathan',
    description='Phonepe Pulse Data visualization using Streamlit and Plotly',
    url='https://github.com/codewithselva/data-visualization-plotly',
)
