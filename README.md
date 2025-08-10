# stock_analyzer_bot
A comprehensive system for extracting financial data, performing advanced analytics, and generating actionable trading insights - powered by Django, Celery, TimescaleDB, Jupyter, OpenAI, and few more.

![alt text](https://github.com/dawmro/stock_analyzer_bot/blob/main/image/chart_1.PNG)


## Setup:
1. Create new virtual env:
``` sh
py -3.12 -m venv venv
```
2. Activate your virtual env:
``` sh
venv/Scripts/activate
```
3. Install packages from included requirements.txt:
``` sh
pip install -r .\requirements.txt
```
4. Run docker container:
```
docker compose up -d
```
5. Go to src dir
```
cd src
```
6. Apply database migrations:
```
python manage.py migrate
```
7. Create admin user:
```
python manage.py createsuperuser
```

## Run
1. Run django server:
```
python manage.py runserver
```
2. In a new terminal run celery beat:
```
celery -A sab_home beat
```
3. In a new terminal run celery:
```
celery -A sab_home worker -l info
```
4. Create periodic tasks in django admin:
5. Wait for task to synchronize data.
6. Got to:
```
http://localhost:8000/market/chart/
```



