
export type Severity = 'High' | 'Medium' | 'Low';

export interface Anomaly {
  id: string;
  timestamp: number;
  metric: string;
  severity: Severity;
  vendor_id: string;
  count: number;
  score: number;
  details: string;
  isNew?: boolean;
}
