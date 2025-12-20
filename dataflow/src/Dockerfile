FROM gcr.io/dataflow-templates-base/python310-template-launcher-base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME (Optional, but recommended)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/confluent_to_bq.py"

COPY . /template/

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r /template/requirements.txt

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
