import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('app.cfg')

IAM_ROLE = config.get("S3", "IAM_ROLE")
DATA_COUNTRY = config.get("S3", "DATA_COUNTRY")
DATA_COVID_DATE = config.get("S3", "DATA_COVID_DATE")
DATA_GDP = config.get("S3", "DATA_GDP")
LOG_COVID = config.get("S3", "LOG_COVID")

# DROP TABLES
table_drop_covid = "DROP TABLE IF EXISTS covid_events"
table_drop_gdp = "DROP TABLE IF EXISTS gdp_dim"
table_drop_country = "DROP TABLE IF EXISTS country_dim"
table_drop_country = "DROP TABLE IF EXISTS country_dim"
table_drop_covid_date = "DROP TABLE IF EXISTS covid_date_dim"


# CREATE TABLES
table_create_covid_events = """
CREATE TABLE IF NOT EXISTS covid_events(
  entity          VARCHAR,
  code            VARCHAR,
  day       VARCHAR,
  total_tests          INTEGER,
  annotations   VARCHAR
);
"""

#countrycode, shortname, tablename, longname, alpha2code, currencyunit, specialnotes, region, incomegroup, wb2code
table_create_dim_country = """
CREATE TABLE IF NOT EXISTS country_dim(
  countrycode          VARCHAR,
  shortname            VARCHAR,
  tablename       VARCHAR,
  longname          VARCHAR,
  alpha2code   VARCHAR,
  currencyunit   VARCHAR,
  specialnotes   VARCHAR,
  region   VARCHAR,
  incomegroup   VARCHAR,
  wb2code   VARCHAR,
  PRIMARY KEY(countrycode)
);
"""

table_create_dim_gdp = """
CREATE TABLE IF NOT EXISTS gdp_dim(
  country     VARCHAR,
  ranking          INTEGER,
  economy          VARCHAR,
  dollars          INTEGER,
  PRIMARY KEY(country)
);
"""

table_create_dim_covid_date = """
CREATE TABLE IF NOT EXISTS covid_date_dim(
  date        VARCHAR,
  year        INTEGER,
  month       INTEGER,
  day         INTEGER,
  weekday     INTEGER,
  day_of_year INTEGER,
  PRIMARY KEY(date)
);
"""

covid_events_copy = ("""
    COPY covid_events FROM {}
    IAM_ROLE {}
    csv
    IGNOREHEADER 1;
""").format(LOG_COVID, IAM_ROLE)

country_dim_copy = ("""
    COPY country_dim
    FROM {}
    IAM_ROLE {}
    csv
    IGNOREHEADER 1;
""").format(DATA_COUNTRY, IAM_ROLE)

covid_date_dim_copy = ("""
    COPY covid_date_dim FROM {}
    IAM_ROLE {}
    csv
    IGNOREHEADER 1;
""").format(DATA_COVID_DATE, IAM_ROLE)

gdp_dim_copy = ("""
    COPY gdp_dim FROM {}
    IAM_ROLE {}
    csv
    IGNOREHEADER 1;
""").format(DATA_GDP, IAM_ROLE)


# QUERY LISTS
drop_table_queries = [table_drop_covid, table_drop_gdp, table_drop_country, table_drop_covid_date]
create_table_queries = [table_create_covid_events, table_create_dim_country, table_create_dim_gdp, table_create_dim_covid_date]
insert_table_queries = [covid_events_copy, country_dim_copy, covid_date_dim_copy, gdp_dim_copy]
# insert_table_queries = [covid_events_copy, covid_date_dim_copy, gdp_dim_copy]
