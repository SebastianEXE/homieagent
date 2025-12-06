/**
 * Scheduling Agent Worker
 * =======================
 * 
 * HOW TO RUN:
 * -----------
 * 1. Make sure dependencies are installed: npm install
 * 2. Start the worker: npm run start:worker
 * 
 * WHAT IT DOES:
 * -------------
 * - Connects to Kafka and listens for incoming messages
 * - Extracts availability from messages like "free at 3pm"
 * - Stores availability in memory (userAvailability object)
 * - When two users have matching times, sends a match notification via API
 */

const { Kafka } = require("kafkajs");
const axios = require("axios");

// ============================================================================
// CONFIGURATION (loaded from environment variables)
// ============================================================================

// Load environment variables from .env file
require("dotenv").config();

const KAFKA_CONFIG = {
  brokers: [process.env.KAFKA_BROKER || "pkc-619z3.us-east1.gcp.confluent.cloud:9092"],
  topic: process.env.KAFKA_TOPIC,
  groupId: process.env.KAFKA_GROUP_ID,
  username: process.env.KAFKA_USERNAME,
  password: process.env.KAFKA_PASSWORD,
};

const API_CONFIG = {
  endpoint: process.env.API_ENDPOINT || "https://series-hackathon-service-202642739529.us-east1.run.app/api/chats",
  apiKey: process.env.API_KEY,
  senderPhone: process.env.SENDER_PHONE,
};

// ============================================================================
// IN-MEMORY STORAGE (no database)
// ============================================================================

// Stores availability: { phoneNumber: "time" }
// Example: { "+15551234567": "3pm", "+15559876543": "5pm" }
let userAvailability = {};

// Logs for the dashboard
let logs = [];

function addLog(type, message, data = {}) {
  const logEntry = {
    id: Date.now(),
    timestamp: new Date().toISOString(),
    type,
    message,
    data,
  };
  logs.push(logEntry);
  // Keep only last 100 logs
  if (logs.length > 100) logs.shift();
  console.log(`[${type.toUpperCase()}] ${message}`, data);
}

// ============================================================================
// MESSAGE PARSING
// ============================================================================

/**
 * Extract availability time from message
 * Matches patterns like: "free at 3pm", "free at 10:30am", "free at noon"
 */
function extractAvailability(text) {
  const pattern = /free at\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm)?|noon|midnight)/i;
  const match = text.match(pattern);
  return match ? match[1].toLowerCase().trim() : null;
}

/**
 * Normalize time for matching (e.g., "3pm" and "3:00pm" should match)
 */
function normalizeTime(time) {
  return time.replace(/\s+/g, "").toLowerCase();
}

// ============================================================================
// MATCHING LOGIC
// ============================================================================

/**
 * Find users with matching availability times
 */
function findMatches(currentPhone, currentTime) {
  const normalizedCurrent = normalizeTime(currentTime);
  const matches = [];

  for (const [phone, time] of Object.entries(userAvailability)) {
    if (phone !== currentPhone && normalizeTime(time) === normalizedCurrent) {
      matches.push({ phone, time });
    }
  }

  return matches;
}

/**
 * Send match notification via Series.so API
 * POST /api/chats - Creates a chat and sends initial message
 */
async function sendMatchNotification(user1Phone, user2Phone, time) {
  const messageText = `🎉 Match found! You and ${user2Phone} are both free at ${time}. Time to schedule!`;
  
  const requestBody = {
    chat: {
      phone_numbers: [user1Phone],
    },
    message: {
      text: messageText,
    },
    send_from: API_CONFIG.senderPhone,
  };
  
  addLog("system", `Sending API request to ${API_CONFIG.endpoint}`, { body: requestBody });
  
  try {
    const response = await axios.post(
      API_CONFIG.endpoint,
      requestBody,
      {
        headers: {
          "Authorization": `Bearer ${API_CONFIG.apiKey}`,
          "Content-Type": "application/json",
        },
      }
    );

    addLog("match", `✅ Notification sent to ${user1Phone}`, {
      matchedWith: user2Phone,
      time,
      status: response.status,
    });

    return response.data;
  } catch (error) {
    // Log full error details for debugging
    const errorDetails = error.response ? {
      status: error.response.status,
      statusText: error.response.statusText,
      data: error.response.data,
    } : { message: error.message };
    
    addLog("error", `Failed to send notification to ${user1Phone}`, errorDetails);
    throw error;
  }
}

// ============================================================================
// KAFKA CONSUMER
// ============================================================================

const kafka = new Kafka({
  clientId: "scheduling-agent",
  brokers: KAFKA_CONFIG.brokers,
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: KAFKA_CONFIG.username,
    password: KAFKA_CONFIG.password,
  },
});

const consumer = kafka.consumer({ groupId: KAFKA_CONFIG.groupId });

async function processMessage(message) {
  try {
    const value = message.value?.toString();
    if (!value) return;

    let parsed;
    try {
      parsed = JSON.parse(value);
    } catch {
      // Not JSON, treat as plain text
      parsed = { text: value, phone: "unknown" };
    }

    // Series.so API v2 message format - extract from nested 'data' object
    const data = parsed.data || parsed;
    
    // Extract text content - Series uses 'text' inside data
    const text = data.text || data.message || data.body || parsed.text || value;
    
    // Extract phone number - Series uses 'chat_handles' array with objects containing 'identifier'
    // Each handle looks like: { display_name: 'You', identifier: '+16463029478', is_me: true }
    // We want the sender's phone (the one where is_me is false, or first one if all are is_me)
    let phone = "unknown";
    if (data.chat_handles && data.chat_handles.length > 0) {
      // Find the sender (not "me") - that's who sent the message
      const sender = data.chat_handles.find(h => !h.is_me) || data.chat_handles[0];
      phone = sender.identifier || sender;
    } else if (data.from_handle) {
      phone = data.from_handle.identifier || data.from_handle;
    } else if (data.from) {
      phone = data.from.identifier || data.from;
    } else if (parsed.phone || parsed.from || parsed.sender) {
      phone = parsed.phone || parsed.from || parsed.sender;
    }

    // IMPORTANT: Ignore our own bot messages to prevent infinite loops
    // Our notifications contain "🎉 Match found!" - skip processing these
    if (text.includes("Match found!") || text.includes("Time to schedule!")) {
      addLog("system", `Ignoring bot message (prevents infinite loop)`);
      return;
    }

    addLog("message", `Received from ${phone}: ${text.substring(0, 100)}`, {
      phone,
      raw: JSON.stringify(parsed).substring(0, 200), // Log raw for debugging
    });

    // Check for availability
    const availability = extractAvailability(text);
    if (availability) {
      addLog("availability", `${phone} is free at ${availability}`, {
        phone,
        time: availability,
      });

      // Check for matches BEFORE adding new availability
      const matches = findMatches(phone, availability);

      // Store the availability
      userAvailability[phone] = availability;

      // Notify all matches
      for (const match of matches) {
        addLog("match", `Match found: ${phone} and ${match.phone} at ${availability}`);
        
        // Notify both users
        await sendMatchNotification(phone, match.phone, availability);
        await sendMatchNotification(match.phone, phone, availability);
      }
    }
  } catch (error) {
    addLog("error", `Error processing message: ${error.message}`);
  }
}

async function startConsumer() {
  addLog("system", "Starting Scheduling Agent Worker...");

  try {
    await consumer.connect();
    addLog("system", "Connected to Kafka cluster");

    await consumer.subscribe({ topic: KAFKA_CONFIG.topic, fromBeginning: false });
    addLog("system", `Subscribed to topic: ${KAFKA_CONFIG.topic}`);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        await processMessage(message);
      },
    });

    addLog("system", "Worker is now listening for messages...");
  } catch (error) {
    addLog("error", `Failed to start consumer: ${error.message}`);
    process.exit(1);
  }
}

// ============================================================================
// GRACEFUL SHUTDOWN
// ============================================================================

async function shutdown() {
  addLog("system", "Shutting down worker...");
  await consumer.disconnect();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// ============================================================================
// START THE WORKER
// ============================================================================

startConsumer();

// Export for potential API access
module.exports = { userAvailability, logs };


