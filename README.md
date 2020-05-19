# Google Mobility Index

## Description

In the midst of the COVID-19 epidemic, Google provided some never before made public insights on user mobility. Google published PDF's
and CSV's based upon each country or sub-regions mobility based on their own mapping software. They were able to provide insight on deviations
in traffic for several major industries such as grocery stores, pharmacies, residential, and work places.

The CSV document was the primary concern. Google releases a single CSV on a preset cadence that seems to update weekly. The PDF contains all
of the mobility data for every country they collect on. It does not allow you to download individual CSV's for your specific country or region.
Instead, they suggest that you use their PDF's which for data analytics purposes are not ideal. Enter this API endpoint.

The core use case of this project, is to maintain an up to date version of the mobility CSV and provide an API for quickly gathering data from it.
This is accompliished through Apache PySpark, Pandas, and some Matplotlib. Given the large size of the data set it made sense to refine my skillset with
one of Apache's big data offerings. Much of the PySpark syntax is found throughout the API and used to aggregate data into dataframes.

This project, allowed me to gather some interesting insights on the state of general mobility both here and abroad.

## Installation

| Dependency | Version | Description |
| :--------: | :-----: | ----------- |
| PySpark | 2.4.5 | Commonly used big data library. Allows for python manipulation of Apache Spark which itself is based off hadoop and integrated with the JVM. Enables for CSV to be read into dataframe syntax. |
| Python | 3 | Any version of python 3 should be sufficient so long as it is explicitly compatible with the other dependencies. |
| Pandas | 1.0.3 | Pandas is useful in providing dataframe utilities than Spark does not itself provide. Spark allows for easy translation to pandas dataframes so jumping between the two is efficient. |
| Matplotlib | 3.2.1 | Not used explicitly in final implementaiton but used extensively in experimentation. Useful in graphing and deeper analytics |
| Schedule | Non Specific | Used as a scheduler for pulling the mobility index CSV on a daily cadence. |

## Usage

Use of this application is pretty straight forward. So long as the dependencies are installed, you should be able to execute the application data_api.py application with the following command line arguments...

<i>python3 data_api.py --country "" --state "" --county "" --mobilitySelector ""</i>

Mobility Selector could be any number 1-7. Use the following scale.

//img

Additionally, kick off the pull_mobility.py to run in the background such that your mobility index CSV remains up to date.

## Sample Graph Output

//img
