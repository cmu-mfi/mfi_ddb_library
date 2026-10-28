import React from 'react';

export default function ServiceCard({ id, title, description, isChecked, onToggle }) {
  return (
    <div 
      className={`p-5 rounded-xl border cursor-pointer select-none transition-all duration-150 flex items-start gap-4 ${
        isChecked 
          ? 'bg-neutral-50 border-cmu-red ring-1 ring-cmu-red/30 shadow-sm' 
          : 'bg-white border-neutral-300 hover:border-neutral-400 hover:bg-neutral-50/60'
      }`}
      onClick={onToggle}
    >
      <div className="flex items-center h-5 mt-0.5">
        <input 
          type="checkbox" 
          checked={isChecked} 
          onChange={() => {}} 
          className="w-4 h-4 rounded text-cmu-red bg-white border-neutral-300 focus:ring-cmu-red accent-cmu-red"
        />
      </div>
      <div className="flex flex-col gap-1">
        <h4 className={`font-bold text-base tracking-wide transition-colors ${isChecked ? 'text-cmu-red' : 'text-neutral-900'}`}>
          {title}
        </h4>
        <p className="text-sm text-neutral-500 leading-relaxed font-normal">
          {description}
        </p>
      </div>
    </div>
  );
}