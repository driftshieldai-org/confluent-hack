
import { Anomaly, Severity } from '../types';

const VENDORS = ['vendor_a', 'vendor_b', 'vendor_c', 'vendor_d', 'vendor_e'];
const METRICS = ['cpu_usage', 'memory_leak', 'failed_logins', 'latency_spike', 'disk_full', 'db_query_timeout'];
const SEVERITIES: Severity[] = ['High', 'Medium', 'Low'];

// Generate a single random anomaly
const createRandomAnomaly = (): Omit<Anomaly, 'id' | 'isNew'> => {
  const score = Math.random();
  let severity: Severity;
  if (score > 0.9) {
    severity = 'High';
  } else if (score > 0.6) {
    severity = 'Medium';
  } else {
    severity = 'Low';
  }
  const vendor = VENDORS[Math.floor(Math.random() * VENDORS.length)];
  return {
    timestamp: Date.now() - Math.floor(Math.random() * 10000),
    metric: METRICS[Math.floor(Math.random() * METRICS.length)],
    severity,
    vendor_id: vendor,
    count: Math.floor(Math.random() * 100) + 1,
    score: parseFloat(score.toFixed(2)),
    details: `Anomaly detected on ${vendor}. Score: ${score.toFixed(2)}`,
  };
};

// Maintain a list of anomalies to simulate a real-time feed
let anomalies: Anomaly[] = Array.from({ length: 20 }, (_, i) => {
    const anomalyData = createRandomAnomaly();
    return {
        ...anomalyData,
        id: `${anomalyData.timestamp}-${anomalyData.vendor_id}-${anomalyData.metric}-${i}`,
    };
}).sort((a, b) => b.timestamp - a.timestamp);

export const fetchAnomalies = async (): Promise<Anomaly[]> => {
  return new Promise((resolve) => {
    setTimeout(() => {
      // Simulate real-time updates: remove an old one, add a new one
      if (Math.random() > 0.3) { // 70% chance to update
        if (anomalies.length > 0) {
            anomalies.pop();
        }
        const newAnomalyData = createRandomAnomaly();
        const newAnomaly: Anomaly = {
            ...newAnomalyData,
            id: `${newAnomalyData.timestamp}-${newAnomalyData.vendor_id}-${newAnomalyData.metric}`,
        };
        anomalies.unshift(newAnomaly);
      }
      
      // Keep the list sorted and capped at 100
      anomalies = anomalies
        .sort((a, b) => b.timestamp - a.timestamp)
        .slice(0, 100);

      resolve(JSON.parse(JSON.stringify(anomalies))); // Return a deep copy
    }, 500 + Math.random() * 1000); // Simulate network latency
  });
};
