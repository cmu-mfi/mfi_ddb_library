import React from 'react';
import ServiceCard from './ServiceCard';

const SERVICES_REGISTRY = [
  {
    id: "infra",
    title: "MQTT Broker Node",
    description: "Provides a lightweight, high-performance EMQX MQTT broker. It enables publish-subscribe messaging, supporting real-time data streaming and communication across the DDB pipeline."
  },
  {
    id: "kv",
    title: "Key-Value DB Node",
    description: "Deploys containerized PostgreSQL to store data of type KV. Features a pre-configured database web service and an initialized connector out-of-the-box for low-latency operational telemetry."
  },
  {
    id: "ts",
    title: "Historian DB Node",
    description: "Spins up TimescaleDB (a specialized time-series extension of PostgreSQL). Built specifically to partition and organize high-frequency system machine sessions, metrics, and sensor timelines via hyper-tables. Has preconfigured database web service and initialized connector for real-time telemetry queries and visualizations."
  },
  {
    id: "blob",
    title: "Blob Object Storage DB Node",
    description: "Orchestrates unstructured binary storage tracking arrays via local or network-mounted host filesystems, preventing container storage degradation during massive file ingestion pipelines."
  },
  {
    id: "rws",
    title: "Retrieval Web Service (RWS API)",
    description: "Exposes a centralized RESTful web gateway enabling external clients to run real-time unified cross-service queries. Leverages a PostgreSQL-backed Metadata Store (MDS) to log vital birth and death tracking metrics for every machine session."
  },
    {
    id: "daa",
    title: "Data Adapter App",
    description: "Bridges the core mfi-ddb library with admin clients. Combines a ReactJS frontend to monitor link statuses and connection loops alongside a REST API backend to configure protocol data adapters to initiate streaming."
  }
];

export default function Step1Selection({ selectedServices, toggleService, nextStep }) {
  return (
    <div className="flex-1 flex flex-col min-h-0 justify-between h-full animate-fade">
      
      {/* 1. TITLE CONTAINER (UNIFIED) */}
      <div className="flex-none pb-4">
        <h2 className="text-2xl font-bold text-neutral-900 tracking-tight">Select Services Required</h2>
        <p className="text-base text-neutral-500 mt-1">Select which services you want to initialize on this DDB Pipeline.</p>
      </div>
      
      {/* 2. BODY AREA (SCROLLABLE) */}
      <div className="flex-1 overflow-y-auto pr-2 min-h-0 custom-scrollbar py-2">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {SERVICES_REGISTRY.map((service) => (
            <ServiceCard 
              key={service.id}
              id={service.id} 
              title={service.title} 
              description={service.description} 
              isChecked={!!selectedServices[service.id]} 
              onToggle={() => toggleService(service.id)} 
            />
          ))}
        </div>
      </div>

      {/* 3. BUTTONS ROW (FIXED ANCHOR) */}
      <div className="flex-none flex justify-end pt-4 border-t border-neutral-200 mt-2 bg-white">
        <button 
          onClick={nextStep}
          className="px-6 py-2.5 cursor-pointer bg-cmu-red hover:bg-cmu-red-hover text-white font-semibold text-sm rounded-lg shadow-sm transition active:scale-[0.98]"
        >
          Configure Parameters →
        </button>
      </div>
    </div>
  );
}