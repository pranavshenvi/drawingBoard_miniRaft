let canvas;
let ctx;

let drawing = false;
let currentPoints = [];

let clientId = "client_" + Math.floor(Math.random() * 10000);

function initCanvas() {

    canvas = document.getElementById("board");
    ctx = canvas.getContext("2d");

    canvas.width = 800;
    canvas.height = 500;

    canvas.addEventListener("mousedown", startStroke);
    canvas.addEventListener("mousemove", continueStroke);
    canvas.addEventListener("mouseup", endStroke);
}

function startStroke(e) {

    drawing = true;
    currentPoints = [];

    const point = getPoint(e);
    currentPoints.push(point);

    ctx.beginPath();
    ctx.moveTo(point.x, point.y);
}

function continueStroke(e) {

    if (!drawing) return;

    const point = getPoint(e);
    currentPoints.push(point);

    ctx.lineTo(point.x, point.y);
    ctx.stroke();
}

function endStroke(e) {

    if (!drawing) return;

    drawing = false;

    const color = document.getElementById("colorPicker").value;
    const thickness = document.getElementById("brushSize").value;

    const stroke = createStroke(
        clientId,
        color,
        thickness,
        currentPoints
    );

    console.log("Generated Stroke Message:");
    // console.log(stroke);
    sendStroke(stroke);

    console.log("JSON message:");
    console.log(JSON.stringify(stroke));
}

function getPoint(e) {

    const rect = canvas.getBoundingClientRect();

    return {
        x: e.clientX - rect.left,
        y: e.clientY - rect.top
    };
}
function drawRemoteStroke(stroke) {

    ctx.beginPath();

    const points = stroke.points;

    ctx.moveTo(points[0].x, points[0].y);

    for (let i = 1; i < points.length; i++) {

        ctx.lineTo(points[i].x, points[i].y);

    }

    ctx.strokeStyle = stroke.color;
    ctx.lineWidth = stroke.thickness;

    ctx.stroke();
}