"use client";

import { useState, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Activity, Users, Clock, Zap, Radio, CheckCircle } from "lucide-react";

// Mock data for the dashboard (replace with real API fetch when worker exposes an endpoint)
// Using static timestamps to avoid hydration mismatch between server and client
const mockLogs = [
  {
    id: 1,
    timestamp: "2025-12-06T06:50:00.000Z",
    type: "message",
    message: "Received from +15551234567: Hey, I'm free at 3pm tomorrow",
  },
  {
    id: 2,
    timestamp: "2025-12-06T06:50:05.000Z",
    type: "availability",
    message: "+15551234567 is free at 3pm",
  },
  {
    id: 3,
    timestamp: "2025-12-06T06:50:30.000Z",
    type: "message",
    message: "Received from +15559876543: I'm also free at 3pm!",
  },
  {
    id: 4,
    timestamp: "2025-12-06T06:50:35.000Z",
    type: "availability",
    message: "+15559876543 is free at 3pm",
  },
  {
    id: 5,
    timestamp: "2025-12-06T06:50:40.000Z",
    type: "match",
    message: "Match found: +15551234567 and +15559876543 at 3pm",
  },
  {
    id: 6,
    timestamp: "2025-12-06T06:50:50.000Z",
    type: "system",
    message: "Worker is now listening for messages...",
  },
];

const mockAgents = [
  { id: 1, name: "Scheduling Agent", status: "active", messages: 142 },
  { id: 2, name: "Kafka Consumer", status: "active", messages: 89 },
];

type LogType = "message" | "availability" | "match" | "system" | "error";

interface Log {
  id: number;
  timestamp: string;
  type: LogType;
  message: string;
}

interface Agent {
  id: number;
  name: string;
  status: "active" | "inactive";
  messages: number;
}

function LogIcon({ type }: { type: LogType }) {
  switch (type) {
    case "message":
      return <Radio className="w-4 h-4" />;
    case "availability":
      return <Clock className="w-4 h-4" />;
    case "match":
      return <Zap className="w-4 h-4" />;
    case "system":
      return <Activity className="w-4 h-4" />;
    case "error":
      return <Activity className="w-4 h-4 text-red-500" />;
    default:
      return <Activity className="w-4 h-4" />;
  }
}

function LogBadge({ type }: { type: LogType }) {
  const styles: Record<LogType, string> = {
    message: "bg-neutral-800 text-neutral-300",
    availability: "bg-neutral-800 text-neutral-300",
    match: "bg-white text-black",
    system: "bg-neutral-900 text-neutral-400 border border-neutral-700",
    error: "bg-red-950 text-red-400",
  };

  return (
    <span
      className={`px-2 py-0.5 text-[10px] uppercase tracking-wider font-medium rounded ${styles[type]}`}
    >
      {type}
    </span>
  );
}

function formatTime(timestamp: string) {
  return new Date(timestamp).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export default function Dashboard() {
  const [logs, setLogs] = useState<Log[]>(() => mockLogs as Log[]);
  const [agents] = useState<Agent[]>(() => mockAgents as Agent[]);
  const [isLive, setIsLive] = useState(true);

  // Simulate live updates (only runs on client after mount)
  const [isMounted, setIsMounted] = useState(false);
  
  useEffect(() => {
    setIsMounted(true);
  }, []);

  useEffect(() => {
    if (!isLive || !isMounted) return;

    const interval = setInterval(() => {
      const types: LogType[] = ["message", "availability", "system"];
      const randomType = types[Math.floor(Math.random() * types.length)];
      const newLog: Log = {
        id: Date.now(),
        timestamp: new Date().toISOString(),
        type: randomType,
        message:
          randomType === "message"
            ? "Received new message from Kafka..."
            : randomType === "availability"
            ? "Processing availability update..."
            : "Heartbeat: Worker healthy",
      };

      setLogs((prev) => [newLog, ...prev].slice(0, 20));
    }, 5000);

    return () => clearInterval(interval);
  }, [isLive, isMounted]);

  return (
    <div className="min-h-screen bg-black text-white">
      {/* Header */}
      <header className="border-b border-neutral-800">
        <div className="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 bg-white rounded-lg flex items-center justify-center">
              <Zap className="w-5 h-5 text-black" />
            </div>
            <h1 className="text-lg font-semibold tracking-tight">
              Scheduling Agent
            </h1>
          </div>
          <button
            onClick={() => setIsLive(!isLive)}
            className={`flex items-center gap-2 px-3 py-1.5 rounded-full text-sm transition-colors ${
              isLive
                ? "bg-white text-black"
                : "bg-neutral-800 text-neutral-400"
            }`}
          >
            <span
              className={`w-2 h-2 rounded-full ${
                isLive ? "bg-green-500 animate-pulse" : "bg-neutral-600"
              }`}
            />
            {isLive ? "Live" : "Paused"}
          </button>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-6 py-8">
        {/* Stats Grid */}
        <div className="grid grid-cols-3 gap-4 mb-8">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-neutral-900 border border-neutral-800 rounded-xl p-5"
          >
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 bg-neutral-800 rounded-lg flex items-center justify-center">
                <Users className="w-5 h-5 text-neutral-400" />
              </div>
              <span className="text-neutral-500 text-sm">Active Agents</span>
            </div>
            <p className="text-3xl font-semibold">{agents.length}</p>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.1 }}
            className="bg-neutral-900 border border-neutral-800 rounded-xl p-5"
          >
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 bg-neutral-800 rounded-lg flex items-center justify-center">
                <Zap className="w-5 h-5 text-neutral-400" />
              </div>
              <span className="text-neutral-500 text-sm">Matches Today</span>
            </div>
            <p className="text-3xl font-semibold">
              {logs.filter((l) => l.type === "match").length}
            </p>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="bg-neutral-900 border border-neutral-800 rounded-xl p-5"
          >
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 bg-neutral-800 rounded-lg flex items-center justify-center">
                <Radio className="w-5 h-5 text-neutral-400" />
              </div>
              <span className="text-neutral-500 text-sm">Messages</span>
            </div>
            <p className="text-3xl font-semibold">
              {agents.reduce((acc, a) => acc + a.messages, 0)}
            </p>
          </motion.div>
        </div>

        <div className="grid grid-cols-3 gap-6">
          {/* Active Agents */}
          <motion.div
            initial={{ opacity: 0, x: -20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: 0.3 }}
            className="col-span-1"
          >
            <h2 className="text-sm font-medium text-neutral-400 uppercase tracking-wider mb-4">
              Active Agents
            </h2>
            <div className="space-y-3">
              {agents.map((agent) => (
                <div
                  key={agent.id}
                  className="bg-neutral-900 border border-neutral-800 rounded-xl p-4"
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-medium">{agent.name}</span>
                    <span className="flex items-center gap-1.5 text-xs text-green-500">
                      <CheckCircle className="w-3 h-3" />
                      {agent.status}
                    </span>
                  </div>
                  <p className="text-sm text-neutral-500">
                    {agent.messages} messages processed
                  </p>
                </div>
              ))}
            </div>
          </motion.div>

          {/* Recent Logs */}
          <motion.div
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: 0.4 }}
            className="col-span-2"
          >
            <h2 className="text-sm font-medium text-neutral-400 uppercase tracking-wider mb-4">
              Recent Logs
            </h2>
            <div className="bg-neutral-900 border border-neutral-800 rounded-xl overflow-hidden">
              <div className="max-h-[500px] overflow-y-auto">
                <AnimatePresence mode="popLayout">
                  {logs.map((log, index) => (
                    <motion.div
                      key={log.id}
                      initial={{ opacity: 0, height: 0 }}
                      animate={{ opacity: 1, height: "auto" }}
                      exit={{ opacity: 0, height: 0 }}
                      transition={{ duration: 0.2 }}
                      className={`px-4 py-3 flex items-start gap-3 ${
                        index !== logs.length - 1
                          ? "border-b border-neutral-800"
                          : ""
                      }`}
                    >
                      <div className="w-8 h-8 bg-neutral-800 rounded-lg flex items-center justify-center flex-shrink-0 mt-0.5">
                        <LogIcon type={log.type} />
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 mb-1">
                          <LogBadge type={log.type} />
                          <span className="text-xs text-neutral-600">
                            {formatTime(log.timestamp)}
                          </span>
                        </div>
                        <p className="text-sm text-neutral-300 truncate">
                          {log.message}
                        </p>
                      </div>
                    </motion.div>
                  ))}
                </AnimatePresence>
              </div>
            </div>
          </motion.div>
        </div>
      </main>

      {/* Footer */}
      <footer className="border-t border-neutral-800 mt-12">
        <div className="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between text-sm text-neutral-600">
          <span>Series Hackathon 2025</span>
          <span>Built with Next.js & Kafka</span>
        </div>
      </footer>
    </div>
  );
}
