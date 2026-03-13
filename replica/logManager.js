let strokeLog = [];

function appendStroke(stroke) {

    strokeLog.push(stroke);

    console.log("Current log size:", strokeLog.length);

}

module.exports = { appendStroke };