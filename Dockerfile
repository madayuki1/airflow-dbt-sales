FROM apache/airflow:2.10.1

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential git\
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
RUN apt-get update

# Copy the requirements file
COPY requirements.txt /requirements.txt

USER airflow

# Upgrade pip
RUN pip install --upgrade pip

# Install required Python packages from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Create a .dbt directory in the Airflow home folder
RUN mkdir -p ${AIRFLOW_HOME}/.dbt

# Copy the profiles.yml file into the .dbt directory
COPY .profiles.yml ${AIRFLOW_HOME}/.dbt/profiles.yml