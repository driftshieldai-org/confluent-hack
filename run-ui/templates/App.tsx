
import React from 'react';
import Header from './components/Header';
import StatusIndicator from './components/StatusIndicator';
import AnomalyTable from './components/AnomalyTable';
import { useAnomalies } from './hooks/useAnomalies';

const App = () => {
  const { anomalies, loading, error, lastUpdated } = useAnomalies();

  return (
    <div className="min-h-screen bg-gray-900 text-gray-200 p-4 sm:p-6 lg:p-8">
      <div className="max-w-7xl mx-auto">
        <Header />
        <main>
          <div className="bg-gray-800/50 border border-gray-700/50 rounded-xl p-4 sm:p-6 shadow-2xl backdrop-blur-sm">
            <StatusIndicator lastUpdated={lastUpdated} loading={loading} />
            <AnomalyTable anomalies={anomalies} loading={loading && anomalies.length === 0} error={error} />
          </div>
        </main>
        <footer className="text-center mt-8 text-sm text-gray-500">
          <p>&copy; {new Date().getFullYear()} Anomaly Detection Inc. All rights reserved.</p>
        </footer>
      </div>
    </div>
  );
};

export default App;
