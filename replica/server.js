// const express = require("express");
// const bodyParser = require("body-parser");
// const { appendStroke } = require("./logManager");

// const app = express();
// const PORT = 5001;

// app.use(bodyParser.json());

// app.post("/stroke", (req, res) => {

//     const stroke = req.body;

//     appendStroke(stroke);

//     console.log("Stroke stored in log");

//     res.json({
//         status: "ok",
//         stroke: stroke
//     });

// });

// app.listen(PORT, () => {
//     console.log("Replica running on port", PORT);
// });

const express = require("express");
const axios = require("axios");
const { appendStroke } = require("./logManager");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5001;
const NODE_ID = process.env.NODE_ID || "1";

const peers = [
    "http://replica2:5002",
    "http://replica3:5003"
];

const isLeader = NODE_ID === "1";

app.post("/stroke", async (req, res) => {

    if (!isLeader) {
        return res.status(403).send("Not leader");
    }

    const stroke = req.body;

    appendStroke(stroke);

    for (let peer of peers) {

        try {
            await axios.post(peer + "/appendEntries", stroke);
        } catch (err) {
            console.log("Follower unreachable:", peer);
        }

    }

    res.json({ status: "committed", stroke });

});
app.post("/appendEntries", (req, res) => {

    const stroke = req.body;

    appendStroke(stroke);

    console.log("Replicated stroke received");

    res.json({ status: "ack" });

});
app.listen(PORT, () => {
    console.log(`Replica ${NODE_ID} running on port ${PORT}`);
});