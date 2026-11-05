import { Fragment, useState } from "react";
import { ChevronDown, ChevronRight, RefreshCw } from "lucide-react";

const statusCls = {
  published: "bg-green-100 text-green-800",
  dry_run: "bg-amber-100 text-amber-800",
  failed: "bg-red-100 text-red-800",
};

const formatTime = (iso) => {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
};

export default function SubmissionHistory({ items, onRefresh }) {
  const [open, setOpen] = useState(null);

  return (
    <div className="rounded-lg bg-white p-6 shadow">
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-800">Recent submissions</h2>
        <button type="button" className="btn-secondary px-3 py-1.5" onClick={onRefresh}>
          <RefreshCw size={14} /> Refresh
        </button>
      </div>

      {items.length === 0 ? (
        <p className="py-6 text-center text-sm text-gray-500">Nothing submitted yet.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="border-b border-gray-200 text-xs uppercase tracking-wide text-gray-500">
              <tr>
                <th className="w-8 py-2" />
                <th className="py-2 pr-4 font-medium">Time</th>
                <th className="py-2 pr-4 font-medium">Form</th>
                <th className="py-2 pr-4 font-medium">Status</th>
                <th className="py-2 font-medium">Topic / error</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <Fragment key={item.id}>
                  <tr
                    className="cursor-pointer border-b border-gray-100 hover:bg-gray-50"
                    onClick={() => setOpen(open === item.id ? null : item.id)}
                  >
                    <td className="py-2 text-gray-400">
                      {open === item.id ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                    </td>
                    <td className="whitespace-nowrap py-2 pr-4 text-gray-700">{formatTime(item.created_at)}</td>
                    <td className="py-2 pr-4">
                      {item.form_id} <span className="font-mono text-xs text-gray-400">{item.msg_type}</span>
                    </td>
                    <td className="py-2 pr-4">
                      <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${statusCls[item.status] || "bg-gray-100"}`}>
                        {item.status.replace("_", " ")}
                      </span>
                    </td>
                    <td className={`max-w-xs truncate py-2 text-xs ${item.error ? "text-red-600" : "text-gray-500"}`}>
                      {item.error || item.topic}
                    </td>
                  </tr>
                  {open === item.id && (
                    <tr className="border-b border-gray-100 bg-gray-50">
                      <td colSpan={5} className="p-3">
                        {item.error && <p className="mb-2 text-xs text-red-600">{item.error}</p>}
                        <pre className="max-h-64 overflow-auto rounded border border-gray-200 bg-white p-3 text-xs">
                          {JSON.stringify(item.payload, null, 2)}
                        </pre>
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
