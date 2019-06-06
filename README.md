# Project Overview
This project contains code for the socialsensing project. This project aims to understand behaviors and interdependencies among various critical infrastructures, using publicly available operational data and social media.

An overview of data used within this project is provided in `data/README.txt`.

# Installing
- Requires Python 3 (tested on Anaconda [distribution](https://www.anaconda.com/download/#macos)).
- Requires MongoDB (tested on local [install](https://docs.mongodb.com/manual/installation/#tutorial-installation)).
- Recommend [DB Browser for SQLite](http://sqlitebrowser.org/) as a GUI to view created sqlite databases.
- Recommend [MongoDB Compass](https://www.mongodb.com/download-center#compass) as a GUI to view created MongoDB databases.

See this [Contribution Guide](https://github.com/Tran-Research-Group/mypythonproject/blob/master/CONTRIBUTING.md) for guidelines for contributing to this project. Make sure you replace any references to "myproject" with "twitterinfrastructure"...

## Testing
- You have to start a mongodb instance to successfully run pytest, since some test scripts will create and query a mongodb database.

Note that you may need to run pytest up to three times before the test successfully completes, since some of the tests require a database and tables to exist which are created by other tests. Since no order is specified for running test scripts, you may need to run pytest up to three times.

## Testing
- You have to start a mongodb instance to successfully run pytest, since some test scripts will create and query a mongodb database.

# References
Much of the work analyzing NYC TLC taxi data is based on work by [Dononvan et al.](http://dx.doi.org/10.1016/j.trc.2017.03.002) using an updated dataset available from [NYC TLC](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml).

## Social Media Analytics
- [Twitter Data Analytics by Kumar, Morstatter, and Liu, 2014](https://www.springer.com/us/book/9781461493716) provides a very brief and high-level overview of basic workflows and methods for analyzing Twitter data. Code examples are provided using java.
- Edwin Chen (former Twitter engineer) has an LDA tutorial on his [blog](http://blog.echen.me/2011/08/22/introduction-to-latent-dirichlet-allocation/).

## Modeling
- [Forecasting: Principles and Practice](https://otexts.com/fpp2/) provides a nice overview of basic time series analysis methods.

# License
Example open source licenses:

- [UIUC License](https://otm.illinois.edu/disclose-protect/illinois-open-source-license)
- [MIT License](https://opensource.org/licenses/MIT)
- [FreeBSD License](https://opensource.org/licenses/BSD-2-Clause)
