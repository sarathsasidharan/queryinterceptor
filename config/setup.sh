#!/bin/bash

sudo pip install virtualenv
virtualenv venv
source venv/bin/activate
pip install pyodbc
pip install pandas
