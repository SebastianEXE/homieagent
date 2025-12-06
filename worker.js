/**
 * Scheduling Agent Worker - Weekly Availability Flow
 * ===================================================
 * 
 * HOW TO RUN:
 * -----------
 * 1. Create .env file with your credentials (see .env.example)
 * 2. Install dependencies: npm install
 * 3. Start the worker: npm run start:worker
 * 
 * WHAT IT DOES:
 * -------------
 * - Parses weekly availability from natural language ("Free Monday 2pm-4pm")
 * - Finds overlapping time slots between users
 * - Suggests meeting times with random activities
 * - Handles YES/NO confirmation flows
 * - Creates group chats and sends calendar invites (.ics)
 */

const { Kafka } = require("kafkajs");
const axios = require("axios");
const { v4: uuidv4 } = require("uuid");
const { createEvent } = require("ics");
const http = require("http");
const {
  parse,
  addDays,
  setHours,
  setMinutes,
  startOfWeek,
  format,
  isAfter,
  isBefore,
  addMinutes,
  differenceInMinutes,
  nextMonday,
  nextTuesday,
  nextWednesday,
  nextThursday,
  nextFriday,
  nextSaturday,
  nextSunday,
} = require("date-fns");

// Load environment variables
require("dotenv").config();

// ============================================================================
// CONFIGURATION
// ============================================================================

const KAFKA_CONFIG = {
  brokers: [process.env.KAFKA_BROKER || "pkc-619z3.us-east1.gcp.confluent.cloud:9092"],
  topic: process.env.KAFKA_TOPIC,
  groupId: process.env.KAFKA_GROUP_ID,
  username: process.env.KAFKA_USERNAME,
  password: process.env.KAFKA_PASSWORD,
};

const API_CONFIG = {
  baseUrl: process.env.API_ENDPOINT?.replace("/api/chats", "") || "https://series-hackathon-service-202642739529.us-east1.run.app",
  apiKey: process.env.API_KEY,
  senderPhone: process.env.SENDER_PHONE,
};

// ============================================================================
// STATE MANAGEMENT (In-Memory)
// ============================================================================

/**
 * Sessions store user state and availability
 * Key: Phone number
 * Value: { 
 *   availability: [{ start: Date, end: Date }], 
 *   state: 'IDLE' | 'WAITING_FOR_MATCH' | 'SUGGESTION_PENDING' | 'CONFIRMED',
 *   pendingMatch: { matchId, otherPhone, suggestedTime, activity },
 *   confirmed: boolean,
 *   chatId: number (chat ID for this user, if we have one)
 * }
 */
const sessions = {};

/**
 * Pending matches waiting for confirmation from both parties
 * Key: matchId (uuid)
 * Value: { userA, userB, suggestedTime, activity, confirmedBy: Set() }
 */
const pendingMatches = {};

/**
 * Track processed message IDs to avoid duplicates
 */
const processedMessages = new Set();

/**
 * Track phone numbers that returned 403 (other teams' users)
 */
const blockedPhones = new Set();

// Logs for debugging/dashboard
let logs = [];

// Available activities for random selection
const ACTIVITIES = ["Coffee", "Lunch", "Coding Session", "Walk", "Study Session"];

// ============================================================================
// LOGGING
// ============================================================================

function addLog(type, message, data = {}) {
  const logEntry = {
    id: Date.now(),
    timestamp: new Date().toISOString(),
    type,
    message,
    data,
  };
  logs.push(logEntry);
  if (logs.length > 100) logs.shift();
  console.log(`[${type.toUpperCase()}] ${message}`, Object.keys(data).length ? data : "");
}

// ============================================================================
// SESSION HELPERS
// ============================================================================

function getSession(phone) {
  if (!sessions[phone]) {
    sessions[phone] = {
      availability: [],
      state: "IDLE",
      pendingMatch: null,
      confirmed: false,
      lastHintTime: 0,
    };
  }
  return sessions[phone];
}

function resetSession(phone) {
  sessions[phone] = {
    availability: [],
    state: "IDLE",
    pendingMatch: null,
    confirmed: false,
    lastHintTime: 0,
  };
}

// ============================================================================
// DATE PARSING - Convert natural language to Date objects
// ============================================================================

/**
 * Get the next occurrence of a day of the week
 */
function getNextDayOfWeek(dayName) {
  const now = new Date();
  const dayMap = {
    monday: nextMonday,
    tuesday: nextTuesday,
    wednesday: nextWednesday,
    thursday: nextThursday,
    friday: nextFriday,
    saturday: nextSaturday,
    sunday: nextSunday,
    mon: nextMonday,
    tue: nextTuesday,
    wed: nextWednesday,
    thu: nextThursday,
    fri: nextFriday,
    sat: nextSaturday,
    sun: nextSunday,
  };
  
  const getter = dayMap[dayName.toLowerCase()];
  return getter ? getter(now) : null;
}

/**
 * Parse time string like "2pm", "14:00", "2:30pm" to { hours, minutes }
 */
function parseTime(timeStr) {
  const str = timeStr.toLowerCase().trim();
  
  // Handle "morning", "afternoon", "evening"
  if (str === "morning") return { hours: 9, minutes: 0 };
  if (str === "afternoon") return { hours: 14, minutes: 0 };
  if (str === "evening") return { hours: 18, minutes: 0 };
  if (str === "noon") return { hours: 12, minutes: 0 };
  
  // Handle "2pm", "2:30pm", "14:00"
  const match = str.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$/i);
  if (!match) return null;
  
  let hours = parseInt(match[1], 10);
  const minutes = match[2] ? parseInt(match[2], 10) : 0;
  const period = match[3]?.toLowerCase();
  
  if (period === "pm" && hours < 12) hours += 12;
  if (period === "am" && hours === 12) hours = 0;
  
  return { hours, minutes };
}

/**
 * Parse availability message like "Free Monday 2pm-4pm" or "Free Tuesday morning"
 * Returns array of { start: Date, end: Date } objects
 */
function parseAvailability(text) {
  const slots = [];
  const normalizedText = text.toLowerCase();
  
  // Pattern: "free [day] [start]-[end]" or "free [day] [time period]"
  // Examples: "free monday 2pm-4pm", "free tuesday morning", "free wed 9am-12pm"
  const patterns = [
    // "free monday 2pm-4pm" or "free monday 2pm to 4pm"
    /free\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|wed|thu|fri|sat|sun)\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\s*(?:-|to)\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)/gi,
    // "free monday morning/afternoon/evening"
    /free\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|wed|thu|fri|sat|sun)\s+(morning|afternoon|evening|noon)/gi,
    // "monday 2pm-4pm" without "free"
    /(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|wed|thu|fri|sat|sun)\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\s*(?:-|to)\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)/gi,
  ];
  
  // Try each pattern
  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(normalizedText)) !== null) {
      const day = match[1];
      const dayDate = getNextDayOfWeek(day);
      
      if (!dayDate) continue;
      
      let startTime, endTime;
      
      if (match[3]) {
        // Has start and end time
        startTime = parseTime(match[2]);
        endTime = parseTime(match[3]);
      } else {
        // Has time period (morning/afternoon/evening)
        const period = match[2];
        if (period === "morning") {
          startTime = { hours: 9, minutes: 0 };
          endTime = { hours: 12, minutes: 0 };
        } else if (period === "afternoon") {
          startTime = { hours: 12, minutes: 0 };
          endTime = { hours: 17, minutes: 0 };
        } else if (period === "evening") {
          startTime = { hours: 17, minutes: 0 };
          endTime = { hours: 21, minutes: 0 };
        } else if (period === "noon") {
          startTime = { hours: 11, minutes: 30 };
          endTime = { hours: 13, minutes: 30 };
        }
      }
      
      if (startTime && endTime) {
        const start = setMinutes(setHours(dayDate, startTime.hours), startTime.minutes);
        const end = setMinutes(setHours(dayDate, endTime.hours), endTime.minutes);
        
        if (isAfter(end, start)) {
          slots.push({ start, end });
        }
      }
    }
  }
  
  return slots;
}

// ============================================================================
// MATCHING ALGORITHM
// ============================================================================

/**
 * Find overlapping time slots between two users
 * Returns the earliest overlap that's >= 30 minutes
 */
function findOverlap(slotsA, slotsB) {
  const overlaps = [];
  
  for (const slotA of slotsA) {
    for (const slotB of slotsB) {
      // Find the overlap
      const overlapStart = isAfter(slotA.start, slotB.start) ? slotA.start : slotB.start;
      const overlapEnd = isBefore(slotA.end, slotB.end) ? slotA.end : slotB.end;
      
      // Check if there's an actual overlap
      if (isAfter(overlapEnd, overlapStart)) {
        const durationMins = differenceInMinutes(overlapEnd, overlapStart);
        if (durationMins >= 30) {
          overlaps.push({
            start: overlapStart,
            end: overlapEnd,
            duration: durationMins,
          });
        }
      }
    }
  }
  
  // Sort by start time and return earliest
  overlaps.sort((a, b) => a.start - b.start);
  return overlaps[0] || null;
}

/**
 * Pick a random activity
 */
function pickActivity() {
  return ACTIVITIES[Math.floor(Math.random() * ACTIVITIES.length)];
}

/**
 * Check all users for potential matches
 */
function checkForMatches(currentPhone) {
  const currentSession = getSession(currentPhone);
  
  if (currentSession.availability.length === 0) return null;
  if (currentSession.state !== "WAITING_FOR_MATCH") return null;
  
  // Find another user who is also waiting for a match
  for (const [otherPhone, otherSession] of Object.entries(sessions)) {
    if (otherPhone === currentPhone) continue;
    if (otherSession.state !== "WAITING_FOR_MATCH") continue;
    if (otherSession.availability.length === 0) continue;
    
    // Check for overlap
    const overlap = findOverlap(currentSession.availability, otherSession.availability);
    
    if (overlap) {
      return {
        otherPhone,
        overlap,
        activity: pickActivity(),
      };
    }
  }
  
  return null;
}

// ============================================================================
// API CALLS - Series.so
// ============================================================================

/**
 * Send a message to a single user
 */
async function sendMessage(toPhone, text) {
  // Skip if this phone is blocked (other team's user)
  if (blockedPhones.has(toPhone)) {
    addLog("system", `Skipping blocked phone ${toPhone}`);
    return null;
  }
  
  try {
    const session = getSession(toPhone);
    
    // Normalize phone number format (ensure it starts with +)
    const normalizedPhone = toPhone.startsWith('+') ? toPhone : `+${toPhone}`;
    
    // Strategy: If we have a chat ID, use chat_messages endpoint (more reliable)
    // Otherwise, create chat and send message in one call
    let response;
    
    if (session.chatId) {
      // Use existing chat - this is more reliable for delivery
      addLog("api", `Using existing chat ${session.chatId} for ${normalizedPhone}`, { 
        textPreview: text.substring(0, 50)
      });
      
      try {
        response = await axios.post(
          `${API_CONFIG.baseUrl}/api/chats/${session.chatId}/chat_messages`,
          {
            message: { text },
          },
          {
            headers: {
              Authorization: `Bearer ${API_CONFIG.apiKey}`,
              "Content-Type": "application/json",
            },
            validateStatus: (status) => status >= 200 && status < 300,
          }
        );
        addLog("api", `Message sent via chat_messages to ${normalizedPhone}`, { 
          status: response.status,
          chatId: session.chatId
        });
      } catch (chatError) {
        // If chat_messages fails, fall back to creating/finding chat
        addLog("api", `Chat ${session.chatId} failed, falling back to /api/chats`, { 
          error: chatError.message,
          status: chatError.response?.status
        });
        session.chatId = null; // Clear invalid chat ID
        // Fall through to create chat below
      }
    }
    
    // Create or find chat and send message in one call
    if (!response) {
      addLog("api", `Creating/finding chat for ${normalizedPhone}`, { 
        textPreview: text.substring(0, 50),
        originalPhone: toPhone,
        senderPhone: API_CONFIG.senderPhone
      });
      
      response = await axios.post(
        `${API_CONFIG.baseUrl}/api/chats`,
        {
          chat: { phone_numbers: [normalizedPhone] },
          message: { text },
          send_from: API_CONFIG.senderPhone,
        },
        {
          headers: {
            Authorization: `Bearer ${API_CONFIG.apiKey}`,
            "Content-Type": "application/json",
          },
          validateStatus: (status) => status >= 200 && status < 300,
        }
      );
    }
    
    // Log full response for debugging
    const responseStr = JSON.stringify(response.data);
    addLog("api", `Full API response for ${normalizedPhone}`, { 
      status: response.status,
      statusText: response.statusText,
      responseKeys: Object.keys(response.data || {}),
      responseData: responseStr.substring(0, 500),
      hasMessage: !!response.data?.message,
      hasChat: !!response.data?.chat
    });
    
    // Extract and store chat ID from response
    const chatId = response.data?.id || 
                   response.data?.chat?.id || 
                   response.data?.chat_id ||
                   response.data?.data?.id;
    
    if (chatId) {
      session.chatId = chatId;
      addLog("api", `Stored chat ID ${chatId} for ${normalizedPhone}`);
    } else {
      addLog("api", `WARNING: No chat ID found in response for ${normalizedPhone}`, { 
        responseData: responseStr.substring(0, 500)
      });
    }
    
    // Check if message was actually created/sent
    // The API returns message info in data.chat_messages (not data.message)
    const messageId = response.data?.data?.chat_messages?.id || 
                      response.data?.message?.id || 
                      response.data?.data?.message?.id ||
                      response.data?.chat_message_id;
    
    const messageText = response.data?.data?.chat_messages?.text;
    const messageSentAt = response.data?.data?.chat_messages?.sent_at;
    
    if (messageId) {
      addLog("api", `Message confirmed sent to ${normalizedPhone}`, { 
        status: response.status, 
        chatId, 
        messageId,
        messageText: messageText?.substring(0, 50),
        sentAt: messageSentAt
      });
    } else {
      addLog("api", `WARNING: No message ID in response for ${normalizedPhone}`, {
        status: response.status,
        chatId,
        responseData: responseStr.substring(0, 500)
      });
    }
    
    // Verify the message text matches what we sent
    if (messageText && messageText !== text) {
      addLog("api", `WARNING: Message text mismatch for ${normalizedPhone}`, {
        sent: text.substring(0, 50),
        received: messageText.substring(0, 50)
      });
    }
    
    addLog("api", `Message sent to ${normalizedPhone}`, { status: response.status, chatId, messageId });
    return response.data;
  } catch (error) {
    // Track 403 errors - these are other teams' users
    if (error.response?.status === 403) {
      blockedPhones.add(toPhone);
      addLog("system", `Blocked phone ${toPhone} (403 - other team's user)`);
      return null;
    }
    // Log full error details for debugging
    addLog("error", `Failed to send message to ${toPhone}`, { 
      error: error.message,
      status: error.response?.status,
      statusText: error.response?.statusText,
      data: error.response?.data,
      url: `${API_CONFIG.baseUrl}/api/chats`
    });
    // Don't throw - return null so the flow continues
    return null;
  }
}

/**
 * Create a group chat with multiple users
 */
async function createGroupChat(phoneNumbers, initialMessage) {
  try {
    const response = await axios.post(
      `${API_CONFIG.baseUrl}/api/chats`,
      {
        chat: { phone_numbers: phoneNumbers },
        message: { text: initialMessage },
        send_from: API_CONFIG.senderPhone,
      },
      {
        headers: {
          Authorization: `Bearer ${API_CONFIG.apiKey}`,
          "Content-Type": "application/json",
        },
      }
    );
    
    // Extract chat ID from various possible response formats
    const chatId = response.data?.id || 
                   response.data?.chat?.id || 
                   response.data?.chat_id ||
                   response.data?.data?.id;
    
    addLog("api", `Group chat created`, { phoneNumbers, chatId, responseKeys: Object.keys(response.data || {}) });
    return { ...response.data, id: chatId };
  } catch (error) {
    addLog("error", `Failed to create group chat`, { error: error.message });
    throw error;
  }
}

/**
 * Send a message with attachment to an existing chat
 */
async function sendAttachment(chatId, text, filename, mimeType, base64Data) {
  try {
    const payload = {
      message: {
        text,
      },
    };
    
    // Only add attachments if provided
    if (filename && mimeType && base64Data) {
      payload.message.attachments = [
        {
          filename,
          mime_type: mimeType,
          data_base64: base64Data,
        },
      ];
    }
    
    const response = await axios.post(
      `${API_CONFIG.baseUrl}/api/chats/${chatId}/chat_messages`,
      payload,
      {
        headers: {
          Authorization: `Bearer ${API_CONFIG.apiKey}`,
          "Content-Type": "application/json",
        },
      }
    );
    addLog("api", `Message sent to chat ${chatId}`, { filename: filename || "text only", status: response.status });
    return response.data;
  } catch (error) {
    const errorDetails = {
      chatId,
      error: error.message,
      status: error.response?.status,
      data: error.response?.data,
    };
    addLog("error", `Failed to send attachment`, errorDetails);
    throw error;
  }
}

// ============================================================================
// CALENDAR INVITE GENERATION
// ============================================================================

/**
 * Generate an .ics calendar file for the meetup
 */
function generateCalendarInvite(title, startDate, durationMinutes, description) {
  return new Promise((resolve, reject) => {
    // Calculate end time
    const endDate = addMinutes(startDate, durationMinutes);
    
    const event = {
      start: [
        startDate.getFullYear(),
        startDate.getMonth() + 1,
        startDate.getDate(),
        startDate.getHours(),
        startDate.getMinutes(),
      ],
      end: [
        endDate.getFullYear(),
        endDate.getMonth() + 1,
        endDate.getDate(),
        endDate.getHours(),
        endDate.getMinutes(),
      ],
      title,
      description,
      status: "CONFIRMED",
      busyStatus: "BUSY",
      organizer: { name: "Scheduling Agent", email: "agent@meetup.local" },
      location: "TBD",
    };
    
    createEvent(event, (error, value) => {
      if (error) {
        addLog("error", `Failed to generate calendar invite`, { error: error.message });
        reject(error);
      } else {
        // Ensure value is a string
        const icsString = typeof value === "string" ? value : value?.value || "";
        if (!icsString) {
          reject(new Error("ICS generation returned empty value"));
        } else {
          resolve(icsString);
        }
      }
    });
  });
}

// ============================================================================
// MESSAGE HANDLERS
// ============================================================================

/**
 * Handle availability message
 */
async function handleAvailability(phone, slots) {
  const session = getSession(phone);
  
  // If user already has a pending suggestion, don't process new availability
  if (session.state === "SUGGESTION_PENDING") {
    await sendMessage(phone, "⏳ You already have a pending meetup! Reply YES to confirm or NO to decline first.");
    return;
  }
  
  // Replace availability (don't accumulate duplicates)
  session.availability = slots;
  session.state = "WAITING_FOR_MATCH";
  
  // Deduplicate slots by creating a unique key for each time slot
  const uniqueSlots = [];
  const seen = new Set();
  for (const slot of slots) {
    const key = `${format(slot.start, "EEEE")}-${format(slot.start, "h:mma")}-${format(slot.end, "h:mma")}`;
    if (!seen.has(key)) {
      seen.add(key);
      uniqueSlots.push(slot);
    }
  }
  
  const slotDescriptions = uniqueSlots.map(s => 
    `${format(s.start, "EEEE")} ${format(s.start, "h:mma")}-${format(s.end, "h:mma")}`
  ).join(", ");
  
  addLog("availability", `${phone} is free: ${slotDescriptions}`, { slots: uniqueSlots.length });
  
  // Acknowledge receipt
  await sendMessage(phone, `Got it! I saved your availability:\n${slotDescriptions}\n\nWe'll let you know if anyone's free this week!`);
  
  // Check for matches
  const match = checkForMatches(phone);
  
  if (match) {
    await createMatchSuggestion(phone, match.otherPhone, match.overlap, match.activity);
  }
}

/**
 * Create a match suggestion and notify both users
 */
async function createMatchSuggestion(phoneA, phoneB, overlap, activity) {
  const matchId = uuidv4();
  
  // Pick a specific time within the overlap (start of the overlap)
  const suggestedTime = overlap.start;
  const duration = Math.min(60, overlap.duration); // 1 hour or less
  
  // Store pending match
  pendingMatches[matchId] = {
    userA: phoneA,
    userB: phoneB,
    suggestedTime,
    duration,
    activity,
    confirmedBy: new Set(),
  };
  
  // Update both sessions
  const sessionA = getSession(phoneA);
  const sessionB = getSession(phoneB);
  
  sessionA.state = "SUGGESTION_PENDING";
  sessionA.pendingMatch = { matchId, otherPhone: phoneB };
  
  sessionB.state = "SUGGESTION_PENDING";
  sessionB.pendingMatch = { matchId, otherPhone: phoneA };
  
  const timeStr = format(suggestedTime, "EEEE, MMMM do 'at' h:mma");
  
  addLog("match", `Match found! ${phoneA} & ${phoneB} - ${activity} on ${timeStr}`);
  
  // Format phone number for display (show last 4 digits)
  const formatPhoneForDisplay = (phone) => {
    // Remove + and extract last 10 digits
    const digits = phone.replace(/\D/g, '');
    if (digits.length >= 10) {
      const last10 = digits.slice(-10);
      return `(${last10.slice(0, 3)}) ${last10.slice(3, 6)}-${last10.slice(6)}`;
    }
    return phone;
  };
  
  const phoneADisplay = formatPhoneForDisplay(phoneB);
  const phoneBDisplay = formatPhoneForDisplay(phoneA);
  
  // Notify both users with personalized messages
  const messageA = ` You and ${phoneADisplay} are both free!\n\n` +
    `**${activity}**\n` +
    `When: ${timeStr}\n` +
    `Duration: ${duration} minutes\n\n` +
    `Reply YES if you are down or NO to decline.`;
  
  const messageB = ` You and ${phoneBDisplay} are both free!\n\n` +
    `**${activity}**\n` +
    `When: ${timeStr}\n` +
    `Duration: ${duration} minutes\n\n` +
    `Reply YES if you are down or NO to decline.`;
  
  await sendMessage(phoneA, messageA);
  await sendMessage(phoneB, messageB);
}

/**
 * Handle YES confirmation
 */
async function handleYesConfirmation(phone) {
  const session = getSession(phone);
  
  // Check if there's a pending match in the session
  if (!session.pendingMatch || !session.pendingMatch.matchId) {
    // Maybe the match was already finalized - check if there's a recent match
    // Look for any pending match involving this user
    for (const [matchId, match] of Object.entries(pendingMatches)) {
      if (match.userA === phone || match.userB === phone) {
        // Found a match - restore the session state
        session.state = "SUGGESTION_PENDING";
        session.pendingMatch = { 
          matchId, 
          otherPhone: match.userA === phone ? match.userB : match.userA 
        };
        break;
      }
    }
  }
  
  if (session.state !== "SUGGESTION_PENDING" || !session.pendingMatch || !session.pendingMatch.matchId) {
    await sendMessage(phone, "No pending meetup to confirm. Share your availability first! (e.g., 'Free Monday 2pm-4pm')");
    return;
  }
  
  const { matchId } = session.pendingMatch;
  const match = pendingMatches[matchId];
  
  if (!match) {
    await sendMessage(phone, "This meetup is no longer available. Share new availability to find another match.");
    resetSession(phone);
    return;
  }
  
  // Check if already confirmed
  if (match.confirmedBy.has(phone)) {
    if (match.confirmedBy.has(match.userA) && match.confirmedBy.has(match.userB)) {
      await sendMessage(phone, "Both of you are already confirmed! The group chat should be created soon.");
    } else {
      await sendMessage(phone, "You're already confirmed! Waiting for the other person...");
    }
    return;
  }
  
  // Record confirmation
  match.confirmedBy.add(phone);
  session.confirmed = true;
  
  addLog("confirm", `${phone} confirmed meetup ${matchId}`, { confirmedCount: match.confirmedBy.size });
  
  // Check if both users confirmed
  if (match.confirmedBy.has(match.userA) && match.confirmedBy.has(match.userB)) {
    await finalizeMeetup(matchId);
  } else {
    await sendMessage(phone, "You're confirmed! Waiting for the other person to confirm...");
  }
}

/**
 * Handle NO/decline
 */
async function handleDecline(phone) {
  const session = getSession(phone);
  
  if (session.state !== "SUGGESTION_PENDING" || !session.pendingMatch) {
    await sendMessage(phone, "No pending meetup to decline.");
    return;
  }
  
  const { matchId, otherPhone } = session.pendingMatch;
  const match = pendingMatches[matchId];
  
  addLog("decline", `${phone} declined meetup ${matchId}`);
  
  // Notify the other person
  if (match) {
    await sendMessage(otherPhone, "The other person declined the meetup. We'll let you know if anyone else is free this week!");
    resetSession(otherPhone);
  }
  
  // Clean up
  delete pendingMatches[matchId];
  resetSession(phone);
  
  await sendMessage(phone, "No problem! We'll let you know if anyone else is free this week!");
}

/**
 * Finalize the meetup - create group chat and send calendar invite
 */
async function finalizeMeetup(matchId) {
  const match = pendingMatches[matchId];
  if (!match) return;
  
  const { userA, userB, suggestedTime, duration, activity } = match;
  const timeStr = format(suggestedTime, "EEEE, MMMM do 'at' h:mma");
  
  addLog("finalize", `Finalizing meetup ${matchId}`, { userA, userB, activity, time: timeStr });
  
  try {
    // Create the combined message
    const groupMessage = `Save the Date!\n\n` +
      `**${activity}**\n` +
      `When: ${timeStr}\n` +
      `Duration: ${duration} minutes\n\n` +
      `You two are all set!`;
    
    // 1. Create group chat with the combined message
    const chatResponse = await createGroupChat([userA, userB], groupMessage);
    const chatId = chatResponse?.id;
    
    addLog("debug", `Chat response`, { chatId, responseData: JSON.stringify(chatResponse).substring(0, 200) });
    
    // 2. Generate calendar invite
    const icsContent = await generateCalendarInvite(
      `${activity} Meetup`,
      suggestedTime,
      duration,
      `Scheduled via Scheduling Agent. Have fun!`
    );
    
    // 3. Send calendar invite as attachment (if we got a chat ID)
    if (chatId) {
      try {
        // Ensure ICS content is valid
        if (!icsContent || icsContent.length === 0) {
          throw new Error("ICS content is empty");
        }
        
        // Validate ICS starts with BEGIN:VCALENDAR
        if (!icsContent.includes("BEGIN:VCALENDAR")) {
          addLog("error", `Invalid ICS content`, { 
            preview: icsContent.substring(0, 100) 
          });
          throw new Error("Invalid ICS format");
        }
        
        const base64Ics = Buffer.from(icsContent).toString("base64");
        
        addLog("debug", `Sending calendar attachment`, { 
          chatId,
          icsLength: icsContent.length,
          base64Length: base64Ics.length,
          icsPreview: icsContent.substring(0, 200)
        });
        
        // Try different mime types - API rejected "text/calendar", try "application/ics"
        await sendAttachment(
          chatId,
          "Here's your calendar invite! Add it to your calendar.",
          "meetup.ics",
          "application/ics",
          base64Ics
        );
        addLog("success", `Calendar invite sent!`, { chatId });
      } catch (attachError) {
        // Calendar attachment failed, but group chat was already created with the message
        const errorDetails = attachError.response?.data || attachError.message;
        addLog("warn", `Calendar attachment failed`, { 
          chatId, 
          error: attachError.message,
          status: attachError.response?.status,
          response: JSON.stringify(errorDetails).substring(0, 200)
        });
        
        // Try to log the actual validation errors
        if (attachError.response?.data?.errors) {
          addLog("error", `Calendar attachment validation errors`, { 
            errors: attachError.response.data.errors 
          });
        }
        // Don't send a text fallback - the group message already has all the info
      }
    } else {
      // No chat ID - send as individual messages
      addLog("warn", `No chatId, sending calendar info as text`);
      await sendMessage(userA, groupMessage);
      await sendMessage(userB, groupMessage);
    }
    
    addLog("success", `Meetup finalized!`, { chatId, matchId });
    
  } catch (error) {
    addLog("error", `Failed to finalize meetup`, { 
      matchId, 
      error: error.message,
      response: error.response?.data,
      status: error.response?.status
    });
    
    // Only say "issue creating group chat" if chat creation actually failed
    const errorMsg = error.response?.status === 422 
      ? "Group chat created, but calendar invite failed. Check the group chat for details!"
      : "There was an issue. Please coordinate directly!";
    
    await sendMessage(userA, errorMsg);
    await sendMessage(userB, errorMsg);
  }
  
  // Clean up
  delete pendingMatches[matchId];
  resetSession(userA);
  resetSession(userB);
}

/**
 * Send help message
 */
async function sendHelp(phone) {
  const helpText = `Hi! I'm your Homie Agent.\n\n` +
    `**How to use:**\n` +
    `1- Tell me when you're free:\n` +
    `   "Free Monday 2pm-4pm"\n` +
    `   "Free Tuesday morning"\n` +
    `   "Wed 9am-12pm"\n\n` +
    `2- I'll match you with someone who's also free at that time.\n\n` +
    `3- Reply YES to confirm or NO to decline.\n\n` +
    `4- Once both confirm, I'll create a group chat and send a calendar invite!\n\n` +
    `**Commands:**\n` +
    `• "status" - Check your current availability\n` +
    `• "clear" - Reset your availability\n` +
    `• "help" - Show this message`;
  
  await sendMessage(phone, helpText);
}

/**
 * Send status message
 */
async function sendStatus(phone) {
  const session = getSession(phone);
  
  let statusText = `**Your Status**\n\n`;
  statusText += `State: ${session.state}\n`;
  
  if (session.availability.length > 0) {
    statusText += `\n**Availability:**\n`;
    for (const slot of session.availability) {
      statusText += `• ${format(slot.start, "EEEE")} ${format(slot.start, "h:mma")}-${format(slot.end, "h:mma")}\n`;
    }
  } else {
    statusText += `\nNo availability set. Tell me when you're free!`;
  }
  
  if (session.pendingMatch) {
    statusText += `\n\nYou have a pending meetup suggestion.`;
  }
  
  await sendMessage(phone, statusText);
}

// ============================================================================
// MAIN MESSAGE PROCESSOR
// ============================================================================

async function processMessage(message) {
  try {
    const value = message.value?.toString();
    if (!value) return;
    
    let parsed;
    try {
      parsed = JSON.parse(value);
    } catch {
      parsed = { text: value, phone: "unknown" };
    }
    
    // Extract data from Series.so format
    const data = parsed.data || parsed;
    const text = data.text || data.message || data.body || parsed.text || value;
    
    // Skip if message has no text content (just metadata)
    if (!text || text.trim().length === 0 || text.startsWith('{"api_version"')) {
      return;
    }
    
    // Extract sender phone FIRST (before using it in messageId)
    let phone = "unknown";
    let isFromBot = false;
    if (data.chat_handles && data.chat_handles.length > 0) {
      const sender = data.chat_handles.find(h => !h.is_me);
      const botHandle = data.chat_handles.find(h => h.is_me);
      
      if (sender) {
        phone = sender.identifier || sender;
      } else if (botHandle) {
        // Message is FROM the bot (is_me = true for all handles means it's outgoing)
        isFromBot = true;
        phone = botHandle.identifier || botHandle;
      }
    } else if (data.from_handle) {
      phone = data.from_handle.identifier || data.from_handle;
    }
    
    // Skip if this phone has returned 403 before (other team's user)
    if (blockedPhones.has(phone)) {
      return;
    }
    
    // Filter messages that are clearly FROM our bot
    // When our bot sends messages, they come back through Kafka
    // We need to filter them by content patterns, regardless of sender phone
    const botResponsePatterns = [
      "Got it! I saved your availability",
      "Found a match!",
      "It's official!",
      "You're confirmed!",
      "You're already confirmed",
      "Both of you are already confirmed",
      "Waiting for the other person",
      "Looking for matches",
      "We'll let you know if anyone's free",
      "You and",
      "are both free",
      "Reply YES to confirm",
      "Reply YES if you are down",
      "Calendar Details",
      "You have a pending meetup",
      "The other person declined",
      "No problem! Share new availability",
      "No pending meetup to confirm",
      "I didn't understand that",
      "Your availability has been cleared",
      "How to use:",
      "Save the Date!",
      "Here's your calendar invite",
      "When:",
      "Duration:",
    ];
    
    // Check if message structure indicates it's from bot (all handles are is_me)
    // OR if message contains our bot's signature phrases
    const looksLikeBotMessage = botResponsePatterns.some(pattern => text.includes(pattern));
    
    if (isFromBot || looksLikeBotMessage) {
      return; // Skip messages from our bot (by structure OR content)
    }
    
    // Create a unique message ID for deduplication (AFTER phone is extracted and bot check passed)
    // Use created_at + phone + first 50 chars of text (more chars = better deduplication)
    const timestamp = parsed.created_at || data.created_at || new Date().toISOString();
    const textHash = text.substring(0, 50).replace(/\s+/g, ' ').trim(); // Normalize whitespace
    const messageId = `${timestamp}-${phone}-${textHash}`;
    
    // Deduplicate - if we've seen this exact message, skip it
    // This prevents processing the same Kafka message multiple times
    if (processedMessages.has(messageId)) {
      return; // Skip duplicate silently
    }
    processedMessages.add(messageId);
    // Keep set from growing indefinitely (keep last 2000)
    if (processedMessages.size > 2000) {
      const iterator = processedMessages.values();
      for (let i = 0; i < 1000; i++) {
        processedMessages.delete(iterator.next().value);
      }
    }
    
    addLog("message", `From ${phone}: ${text.substring(0, 100)}`);
    
    const normalizedText = text.toLowerCase().trim();
    
    // Check for commands
    if (normalizedText === "help" || normalizedText === "hi" || normalizedText === "hello") {
      await sendHelp(phone);
      return;
    }
    
    if (normalizedText === "status") {
      await sendStatus(phone);
      return;
    }
    
    if (normalizedText === "clear" || normalizedText === "reset") {
      resetSession(phone);
      await sendMessage(phone, "Your availability has been cleared. Let me know when you're free!");
      return;
    }
    
    // Get session early to check state
    const session = getSession(phone);
    
    // Check for YES confirmation (check this BEFORE availability parsing)
    // This should always be handled, even if state has changed
    if (normalizedText === "yes" || normalizedText === "y" || normalizedText === "confirm") {
      await handleYesConfirmation(phone);
      return; // Always return after handling yes - don't process further
    }
    
    // Check for NO/decline
    if (normalizedText === "no" || normalizedText === "n" || normalizedText === "decline" || normalizedText === "cancel") {
      await handleDecline(phone);
      return;
    }
    
    // If user has pending suggestion, don't accept new availability
    if (session.state === "SUGGESTION_PENDING") {
      const now = Date.now();
      if (!session.lastHintTime || now - session.lastHintTime > 60000) {
        session.lastHintTime = now;
        await sendMessage(phone, "↪You have a pending meetup! Reply YES to confirm or NO to decline.");
      }
      return;
    }
    
    // If user just confirmed (state might be CONFIRMED or transitioning), don't send "didn't understand"
    // This prevents the "I didn't understand" message after confirmation
    if (session.state === "CONFIRMED" || session.confirmed) {
      // User just confirmed - don't process any other messages until match is finalized
      return;
    }
    
    // Try to parse availability
    const slots = parseAvailability(text);
    
    if (slots.length > 0) {
      await handleAvailability(phone, slots);
      return;
    }
    
    // Unknown message - send hint (but throttle to prevent spam)
    const now = Date.now();
    
    // Only send "didn't understand" once per minute per user
    if (!session.lastHintTime || now - session.lastHintTime > 60000) {
      session.lastHintTime = now;
      
      await sendMessage(phone, 
        `I didn't understand that. Try:\n` +
        `• "Free Monday 2pm-4pm"\n` +
        `• "Free Tuesday morning"\n` +
        `• "help" for instructions`
      );
    }
    
  } catch (error) {
    addLog("error", `Error processing message: ${error.message}`);
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

// Consumer instance (will be recreated after disconnect)
let consumer = kafka.consumer({ groupId: KAFKA_CONFIG.groupId });

// Track if consumer is running
let isConsumerRunning = false;

// Graceful shutdown
let isShuttingDown = false;
let server = null;

// Stop the consumer without exiting the process (for API control)
async function stopConsumer() {
  if (!isConsumerRunning) {
    return;
  }
  
  addLog("system", "Stopping consumer...");
  isConsumerRunning = false;
  try {
    await consumer.disconnect();
    addLog("system", "Consumer stopped");
  } catch (error) {
    console.error("Error disconnecting consumer:", error);
    addLog("error", `Failed to stop consumer: ${error.message}`);
    throw error;
  }
}

// Graceful shutdown function (exits the process - only for signal handlers)
async function shutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  addLog("system", "Shutting down worker...");
  isConsumerRunning = false;
  try {
    await consumer.disconnect();
  } catch (error) {
    console.error("Error disconnecting consumer:", error);
  }
  if (server) {
    server.close();
  }
  process.exit(0);
}

// HTTP Server for control API
server = http.createServer((req, res) => {
  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Content-Type', 'application/json');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === '/status' && req.method === 'GET') {
    res.writeHead(200);
    res.end(JSON.stringify({
      running: isConsumerRunning,
      sessions: Object.keys(sessions).length,
      pendingMatches: Object.keys(pendingMatches).length,
      logs: logs.slice(-10), // Last 10 logs
    }));
    return;
  }

  if (url.pathname === '/start' && req.method === 'POST') {
    if (isConsumerRunning) {
      res.writeHead(200);
      res.end(JSON.stringify({ success: true, message: 'Worker is already running' }));
      return;
    }
    
    // Start the consumer if not already running
    startConsumer().then(() => {
      res.writeHead(200);
      res.end(JSON.stringify({ success: true, message: 'Worker started' }));
    }).catch((error) => {
      res.writeHead(500);
      res.end(JSON.stringify({ success: false, error: error.message }));
    });
    return;
  }

  if (url.pathname === '/stop' && req.method === 'POST') {
    if (!isConsumerRunning) {
      res.writeHead(200);
      res.end(JSON.stringify({ success: true, message: 'Worker is not running' }));
      return;
    }
    
    // Stop the consumer but keep the HTTP server running
    stopConsumer().then(() => {
      res.writeHead(200);
      res.end(JSON.stringify({ success: true, message: 'Worker stopped' }));
    }).catch((error) => {
      res.writeHead(500);
      res.end(JSON.stringify({ success: false, error: error.message }));
    });
    return;
  }

  // 404
  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found' }));
});

// Start HTTP server on port 3001
const PORT = 3001;
server.listen(PORT, () => {
  console.log(`[API] Control server listening on http://localhost:${PORT}`);
});

// Update startConsumer to track running state
async function startConsumer() {
  if (isConsumerRunning) {
    addLog("system", "Consumer is already running");
    return;
  }
  
  addLog("system", "Starting Scheduling Agent Worker (Weekly Flow)...");
  
  try {
    // Always recreate consumer when starting (KafkaJS consumers can't reconnect after disconnect)
    consumer = kafka.consumer({ groupId: KAFKA_CONFIG.groupId });
    await consumer.connect();
    
    addLog("system", "Connected to Kafka cluster");
    
    await consumer.subscribe({ topic: KAFKA_CONFIG.topic, fromBeginning: false });
    addLog("system", `Subscribed to topic: ${KAFKA_CONFIG.topic}`);
    
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        await processMessage(message);
      },
    });
    
    isConsumerRunning = true;
    addLog("system", "Worker is now listening for messages...");
  } catch (error) {
    isConsumerRunning = false;
    addLog("error", `Failed to start consumer: ${error.message}`);
    throw error;
  }
}

// Set up signal handlers for graceful shutdown
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// Start the worker automatically
startConsumer().catch((error) => {
  console.error("Failed to start worker:", error);
  process.exit(1);
});

// Export for potential API access
module.exports = { sessions, pendingMatches, logs };

