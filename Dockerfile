FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN apt-get update && \
    apt-get install -y \
        gnupg2 \
        curl

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get install -y unixodbc-dev && \
    apt-get install -y libgssapi-krb5-2

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

CMD ["python", "bot.py"]