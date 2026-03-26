let log = [];

function appendEntry(term, stroke) {
    const index = log.length;
    const entry = {
        index: index,
        term: term,
        stroke: stroke,
        timestamp: Date.now()
    };
    log.push(entry);
    console.log(`Log entry ${index} (term ${term}) appended`);
    return entry;
}

function appendStroke(stroke) {
    // Legacy function for backward compatibility
    return appendEntry(0, stroke);
}

function getEntry(index) {
    if (index < 0 || index >= log.length) return null;
    return log[index];
}

function getLastLogIndex() {
    return log.length - 1;  // -1 if empty
}

function getLastLogTerm() {
    if (log.length === 0) return 0;
    return log[log.length - 1].term;
}

function getEntriesFrom(startIndex) {
    if (startIndex < 0) startIndex = 0;
    if (startIndex >= log.length) return [];
    return log.slice(startIndex);
}

function deleteEntriesFrom(index) {
    if (index < log.length && index >= 0) {
        console.log(`Deleting entries from index ${index}`);
        log = log.slice(0, index);
    }
}

function getLog() {
    return log;
}

function getLogLength() {
    return log.length;
}

module.exports = {
    appendEntry,
    appendStroke,
    getEntry,
    getLastLogIndex,
    getLastLogTerm,
    getEntriesFrom,
    deleteEntriesFrom,
    getLog,
    getLogLength
};
