import React, { useState } from 'react';
import { Bar, Line } from 'recharts';

const DashboardDemo = () => {
  const [activeTab, setActiveTab] = useState('performance');
  
  return (
    <div className="flex flex-col h-full bg-gray-50">
      {/* Header */}
      <div className="bg-blue-600 text-white p-4 flex justify-between items-center">
        <div className="text-xl font-bold">Campaign Intelligence Dashboard</div>
        <div className="flex space-x-2">
          <button className="bg-white text-blue-600 px-3 py-1 rounded-md text-sm font-medium">Export</button>
          <button className="bg-white text-blue-600 px-3 py-1 rounded-md text-sm font-medium">Settings</button>
        </div>
      </div>
      
      {/* Navigation */}
      <div className="bg-white border-b flex">
        <button 
          className={`px-4 py-3 font-medium ${activeTab === 'performance' ? 'text-blue-600 border-b-2 border-blue-600' : 'text-gray-600'}`}
          onClick={() => setActiveTab('performance')}
        >
          Performance
        </button>
        <button 
          className={`px-4 py-3 font-medium ${activeTab === 'audience' ? 'text-blue-600 border-b-2 border-blue-600' : 'text-gray-600'}`}
          onClick={() => setActiveTab('audience')}
        >
          Audience Analysis
        </button>
        <button 
          className={`px-4 py-3 font-medium ${activeTab === 'optimization' ? 'text-blue-600 border-b-2 border-blue-600' : 'text-gray-600'}`}
          onClick={() => setActiveTab('optimization')}
        >
          AI Optimization
        </button>
        <button 
          className={`px-4 py-3 font-medium ${activeTab === 'trends' ? 'text-blue-600 border-b-2 border-blue-600' : 'text-gray-600'}`}
          onClick={() => setActiveTab('trends')}
        >
          Trend Analysis
        </button>
      </div>
      
      {/* Main Content */}
      <div className="flex flex-1 p-4 gap-4">
        {/* Left Panel - Filters */}
        <div className="w-64 bg-white p-4 rounded-lg shadow">
          <h3 className="font-semibold text-lg mb-4">Filters</h3>
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-1">Date Range</label>
            <select className="w-full border rounded-md p-2 text-sm">
              <option>Last 30 days</option>
              <option>Last quarter</option>
              <option>Last year</option>
              <option>Custom range</option>
            </select>
          </div>
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-1">Companies</label>
            <select className="w-full border rounded-md p-2 text-sm">
              <option>All Companies</option>
              <option>Aura Align</option>
              <option>Hearth Harmony</option>
              <option>Cyber Circuit</option>
              <option>Well Wish</option>
            </select>
          </div>
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-1">Channels</label>
            <div className="space-y-1">
              <div className="flex items-center">
                <input type="checkbox" className="mr-2" defaultChecked />
                <span className="text-sm">Facebook</span>
              </div>
              <div className="flex items-center">
                <input type="checkbox" className="mr-2" defaultChecked />
                <span className="text-sm">Instagram</span>
              </div>
              <div className="flex items-center">
                <input type="checkbox" className="mr-2" defaultChecked />
                <span className="text-sm">Pinterest</span>
              </div>
              <div className="flex items-center">
                <input type="checkbox" className="mr-2" defaultChecked />
                <span className="text-sm">Twitter</span>
              </div>
            </div>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Campaign Goals</label>
            <div className="space-y-1">
              <div className="flex items-center">
                <input type="checkbox" className="mr-2" defaultChecked />
                <span className="text-sm">Product Launch</span>
              </div>
              <div className="flex items-center">
                <input type="checkbox" className="mr-2" defaultChecked />
                <span className="text-sm">Market Expansion</span>
              </div>
              <div className="flex items-center">
                <input type="checkbox" className="mr-2" defaultChecked />
                <span className="text-sm">Increase Sales</span>
              </div>
            </div>
          </div>
        </div>
        
        {/* Right Panel - Content */}
        <div className="flex-1 space-y-4">
          {/* AI Insights */}
          <div className="bg-white p-4 rounded-lg shadow">
            <div className="flex justify-between items-center mb-2">
              <h3 className="font-semibold text-lg">AI-Powered Insights</h3>
              <button className="text-blue-600 text-sm">Refresh</button>
            </div>
            <div className="bg-blue-50 border border-blue-100 p-3 rounded-md">
              <p className="text-sm text-gray-800">
                <span className="font-semibold">Optimization opportunity detected:</span> Consider reallocating 15% of budget from underperforming Pinterest campaigns for "Men 45-60" to high-ROI Facebook campaigns targeting "Women 45-60" in Los Angeles. Projected ROI increase: +1.2
              </p>
            </div>
          </div>
          
          {/* KPI Cards */}
          <div className="grid grid-cols-4 gap-4">
            <div className="bg-white p-4 rounded-lg shadow">
              <p className="text-sm text-gray-500">Average ROI</p>
              <p className="text-2xl font-semibold">2.41</p>
              <p className="text-xs text-green-600">↑ 0.3 vs previous period</p>
            </div>
            <div className="bg-white p-4 rounded-lg shadow">
              <p className="text-sm text-gray-500">Conversion Rate</p>
              <p className="text-2xl font-semibold">0.08</p>
              <p className="text-xs text-red-600">↓ 0.02 vs previous period</p>
            </div>
            <div className="bg-white p-4 rounded-lg shadow">
              <p className="text-sm text-gray-500">Avg. CPA</p>
              <p className="text-2xl font-semibold">$500.00</p>
              <p className="text-xs text-gray-600">No change vs previous period</p>
            </div>
            <div className="bg-white p-4 rounded-lg shadow">
              <p className="text-sm text-gray-500">Engagement Score</p>
              <p className="text-2xl font-semibold">5.4</p>
              <p className="text-xs text-green-600">↑ 0.7 vs previous period</p>
            </div>
          </div>
          
          {/* Charts */}
          <div className="grid grid-cols-2 gap-4">
            <div className="bg-white p-4 rounded-lg shadow">
              <h4 className="font-medium mb-4">ROI by Channel</h4>
              <div className="h-64 flex items-center justify-center text-gray-400">
                [Bar Chart Visualization]
              </div>
            </div>
            <div className="bg-white p-4 rounded-lg shadow">
              <h4 className="font-medium mb-4">Conversion Rate by Audience</h4>
              <div className="h-64 flex items-center justify-center text-gray-400">
                [Bar Chart Visualization]
              </div>
            </div>
          </div>
          
          {/* LLM Query Interface */}
          <div className="bg-white p-4 rounded-lg shadow">
            <h4 className="font-medium mb-3">Ask Campaign Intelligence</h4>
            <div className="flex">
              <input 
                type="text" 
                className="flex-1 border rounded-l-md px-3 py-2 focus:outline-none focus:ring-1 focus:ring-blue-500"
                placeholder="Ask a question about your campaign data..."
                defaultValue="Which channel has the best ROI for technology segment?"
              />
              <button className="bg-blue-600 text-white px-4 py-2 rounded-r-md">Ask</button>
            </div>
            <div className="mt-3 bg-gray-50 p-3 rounded-md">
              <p className="text-sm text-gray-800">
                <span className="font-semibold">Response:</span> Facebook has the highest average ROI (7.21) for the Technology segment, followed by Instagram (2.81). This is 243% higher than the average ROI across all channels for this segment.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DashboardDemo;