"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { Power, PowerOff } from "lucide-react";

export default function Dashboard() {
  const [isAgentOn, setIsAgentOn] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Check agent status on mount and periodically
  useEffect(() => {
    const checkStatus = async () => {
      try {
        const response = await fetch('/api/agent/status');
        const data = await response.json();
        setIsAgentOn(data.running || false);
      } catch (err) {
        setIsAgentOn(false);
      }
    };

    checkStatus();
    const interval = setInterval(checkStatus, 3000); // Check every 3 seconds
    return () => clearInterval(interval);
  }, []);

  const handleTurnOn = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const response = await fetch('/api/agent/start', { method: 'POST' });
      const data = await response.json();
      if (data.success) {
        setIsAgentOn(true);
      } else {
        setError(data.error || 'Failed to start agent');
      }
    } catch (err: any) {
      setError(err.message || 'Failed to start agent. Make sure the worker is running with: npm run start:worker');
    } finally {
      setIsLoading(false);
    }
  };

  const handleTurnOff = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const response = await fetch('/api/agent/stop', { method: 'POST' });
      const data = await response.json();
      if (data.success) {
        setIsAgentOn(false);
      } else {
        setError(data.error || 'Failed to stop agent');
      }
    } catch (err: any) {
      setError(err.message || 'Failed to stop agent');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-black text-white flex items-center justify-center">
      <div className="max-w-md w-full px-6">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="text-center mb-12"
        >
          <h1 className="text-3xl font-semibold tracking-tight mb-2">
            Homie Agent
          </h1>
          <p className="text-neutral-400 text-sm">
            Control your scheduling agent
          </p>
        </motion.div>

        <div className="space-y-4">
          <motion.button
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.1 }}
            onClick={handleTurnOn}
            disabled={isAgentOn || isLoading}
            className={`w-full py-4 px-6 rounded-xl border-2 transition-all ${
              isAgentOn || isLoading
                ? "bg-neutral-900 border-neutral-800 text-neutral-500 cursor-not-allowed"
                : "bg-white text-black border-white hover:bg-neutral-100 active:scale-95"
            }`}
          >
            <div className="flex items-center justify-center gap-3">
              <Power className="w-5 h-5" />
              <span className="font-semibold">
                {isLoading ? "Starting..." : "Turn On Agent"}
              </span>
            </div>
          </motion.button>

          <motion.button
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            onClick={handleTurnOff}
            disabled={!isAgentOn || isLoading}
            className={`w-full py-4 px-6 rounded-xl border-2 transition-all ${
              !isAgentOn || isLoading
                ? "bg-neutral-900 border-neutral-800 text-neutral-500 cursor-not-allowed"
                : "bg-black text-white border-white hover:bg-neutral-900 active:scale-95"
            }`}
          >
            <div className="flex items-center justify-center gap-3">
              <PowerOff className="w-5 h-5" />
              <span className="font-semibold">
                {isLoading ? "Stopping..." : "Turn Off Agent"}
              </span>
            </div>
          </motion.button>
        </div>

        {error && (
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            className="mt-4 p-4 bg-red-950 border border-red-800 rounded-xl text-center"
          >
            <p className="text-sm text-red-400">{error}</p>
          </motion.div>
        )}

        {isAgentOn && !error && (
          <motion.div
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
            className="mt-8 p-4 bg-neutral-900 border border-neutral-800 rounded-xl text-center"
          >
            <div className="flex items-center justify-center gap-2 text-green-400">
              <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse" />
              <span className="text-sm font-medium">Agent is running</span>
            </div>
          </motion.div>
        )}
      </div>
    </div>
  );
}
