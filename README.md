# Brizzah Deploy Pack

Arquivos:
- app.py
- requirements.txt
- render.yaml
- .env.example

## Rodar local
python -m pip install -r requirements.txt
python app.py

## No Render
Build Command:
pip install -r requirements.txt

Start Command:
gunicorn app:app
