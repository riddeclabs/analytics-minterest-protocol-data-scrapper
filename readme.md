# BI Protocol Data Scrapper

This project scrapes MNT protocol data for BI needs and uploads it to the analytical MySQL database

## Getting started

1. Activate virtual environment
   - Use `virtualenv`, (install if not installed via `pip install virtualenv`)
   - Install virtual environment with python 3.11 running `virtualenv -p python3.11 -v .venv`
   - Allow VSCode to use virtual environment by default
   - Close (by pressing on the trash icon) and reopen the terminal
   - Virtual environment should now be active
2. Install dependencies by running `pip install -r requirements.txt`
3. Run the app using `python src/main.py`
