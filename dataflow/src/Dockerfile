FROM gcr.io/dataflow-templates-base/python310-template-launcher-base


ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/pubsub_to_bq.py"

COPY . /template/

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r /template/requirements.txt

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
