import os
import configparser
from flask import Flask, jsonify, render_template
from google.cloud import bigquery
from flask_cors import CORS
import logging

app = Flask(__name__)
# Enable CORS so your UI can fetch data if hosted separately;
# not strictly necessary if serving static files from the same app.
CORS(app)

# Load configuration
config = configparser.ConfigParser()
config.read('config.ini')
ANOMALIES_TABLE = config['bigquery']['anomalies_table']

# Cloud Run automatically uses the service account attached to the revision
client = bigquery.Client()

@app.route('/')
def index():
    # This looks for 'index.html' inside the 'templates' folder
    return render_template('index.html')

@app.route('/api/anomalies', methods=['GET'])
def get_anomalies():
    query = f"""
        SELECT 
            timestamp,
            Metric,
            severity,
            vendor_id,
            count,
            score,
            details
        FROM 
            `{ANOMALIES_TABLE}`
        ORDER BY 
            timestamp DESC
        LIMIT 100
    """

    try:
        query_job = client.query(query)
        results = []
        for row in query_job:
            results.append({
                "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None,
                "metric": row["Metric"],
                "severity": row["severity"],
                "vendor_id": row["vendor_id"],
                "count": row["count"],
                "score": row["score"],
                "details": row["details"]
            })
        logging.info(json.dumps({
                "identifier": "anomaly_summary",
                "summary": "testing mail",
                "anomaly_count": 1
            }))
        
        return jsonify(results), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
