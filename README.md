# opendicomMWL - (dcm4chee-arc-ligth 5.6.0 - MySQL)

## Create virtual environment

virtualenv --python=python3.6 --no-site-packages env

Install requirement

pip install -r requirements.txt

## Parameters
- AETitle
- Port
- User DB
- Password DB
- Ip DB
- Name DB
- debug (1 = True, 0 = False)

## Example 
- python opendicomMWL/main.py AET 11112 USER_DB PASSWORD_DB 0.0.0.0 NAME_DB 1