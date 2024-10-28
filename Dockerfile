FROM apache/airflow:2.10.0

# Install required Python packages
COPY requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt \
    && pip install google-cloud-bigquery google-auth

# Download and install Google Cloud SDK
USER root

RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz -o /tmp/google-cloud-sdk.tar.gz \
    && mkdir -p /opt/gcloud \
    && tar -C /opt/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
    && /opt/gcloud/google-cloud-sdk/install.sh

# Download and extract the city list
RUN mkdir -p /opt/airflow/dags/data && \
    curl -o /opt/airflow/dags/data/city.list.json.gz http://bulk.openweathermap.org/sample/city.list.json.gz && \
    gunzip /opt/airflow/dags/data/city.list.json.gz

# Add Google Cloud SDK to PATH
ENV PATH=$PATH:/opt/gcloud/google-cloud-sdk/bin

# Disable loading examples
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

USER airflow
