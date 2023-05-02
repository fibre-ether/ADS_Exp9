FROM datamechanics/spark:2.4.5-hadoop-3.1.0-java-8-scala-2.11-python-3.7-dm18

ENV PYSPARK_MAJOR_PYTHON_VERSION=3

WORKDIR /opt/application

COPY requirements.txt .

USER root
RUN apt-get update --allow-insecure-repositories
RUN apt-get install build-essential manpages-dev -y
RUN pip3 install --upgrade pip 
RUN pip3 install -r requirements.txt
# RUN pip3 install pandas pyarrow 
# RUN pip3 install -U --upgrade pyspark

COPY . .

CMD ./run.sh
