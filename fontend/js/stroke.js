function createStroke(clientId, color, thickness, points) {

    return {
        type: "stroke",
        client_id: clientId,
        color: color,
        thickness: thickness,
        points: points,
        timestamp: Date.now()
    };

}