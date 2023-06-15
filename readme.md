# Canvas Auto-Shifter

Early in 2023, Instructure released the Canvas Data 2 API, which allows for much easier access to Canvas data. 

This is a simple script that uses SQLAlchemy to push this data to a database. 

It was originally implemented with an Azure PGSQL server (flexible), but could likely be ported to many different 
flavors of database. 

## Installation
To install and run this script, clone the repository and install the requirements with `pip install -r requirements.txt`

## Configuration
This script uses the following environment variables to pull in the required secrets and connection strings: 
- `DAP_API_URL`
- `DAP_CLIENT_ID`
- `DAP_CLIENT_SECRET`
- `DAP_CONNECTION_STRING`

The API URL can be found in the Canvas Data 2 documentation, and Canvas customers can generate a client ID and secret at https://identity.instructure.com

The connection string should be in the following format:
`postgresql://<username>:<password>@<db-server-fqdn>:5432/<db_name>`

The script also uses python-dotenv, so these values can also be put in a `.\.env` file for ease of persistence.

