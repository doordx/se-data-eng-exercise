FROM apache/airflow:2.7.0-python3.10

# Install dbt
RUN pip install dbt-core dbt-bigquery

USER root

# Install the Google Cloud SDK
RUN apt-get update && apt-get install -y apt-transport-https ca-certificates gnupg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update && apt-get install -y google-cloud-sdk

USER airflow
