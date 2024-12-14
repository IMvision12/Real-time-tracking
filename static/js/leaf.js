var mymap = L.map('mapid').setView([40.6872, -73.9424], 13);

L.tileLayer('https://api.mapbox.com/styles/v1/mapbox/streets-v11/tiles/{z}/{x}/{y}?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 18,
    accessToken: 'Your API KEY'
}).addTo(mymap);

// Store bus markers with bus_id as key
var busMarkers = {};
var lastUpdateTime = new Date();
var updateCounter = 0;

// Function to format timestamp
function formatTime(date) {
    return date.toLocaleTimeString('en-US', { 
        hour12: true,
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });
}

// New function for route visualization
function drawBusRoute(busId) {
    // Remove existing route if it exists
    if (busMarkers[busId].route) {
        mymap.removeLayer(busMarkers[busId].route);
    }

    // Get the bus's coordinate history
    const routeCoordinates = busMarkers[busId].getLatLngs().map(latlng => [latlng.lat, latlng.lng]);
    
    if (routeCoordinates.length < 2) return;

    const routeLayer = L.polyline(routeCoordinates, {
        color: 'blue',
        weight: 3,
        opacity: 0.7,
        dashArray: '10, 10'
    }).addTo(mymap);

    // Store route layer with the marker
    busMarkers[busId].route = routeLayer;
}

function updateMarkers(geojsonData) {
    try {
        updateCounter++;
        const currentTime = new Date();
        console.log(`Update #${updateCounter} at ${formatTime(currentTime)}`);
        
        const currentBusIds = new Set(geojsonData.features.map(f => f.properties.bus_id));
        
        Object.keys(busMarkers).forEach(busId => {
            if (!currentBusIds.has(busId)) {
                console.log(`Removing inactive bus: ${busId}`);
                // Remove route if exists
                if (busMarkers[busId].route) {
                    mymap.removeLayer(busMarkers[busId].route);
                }
                mymap.removeLayer(busMarkers[busId]);
                delete busMarkers[busId];
            }
        });
        
        geojsonData.features.forEach(feature => {
            const busId = feature.properties.bus_id;
            const coordinates = feature.geometry.coordinates[0];
            const timestamp = feature.properties.timestamps[feature.properties.timestamps.length - 1];
            
            let speed = 'N/A';
            if (busMarkers[busId]) {
                const prevPos = busMarkers[busId].getLatLng();
                const distance = mymap.distance(
                    [prevPos.lat, prevPos.lng],
                    [coordinates[1], coordinates[0]]
                );
                const timeDiff = (currentTime - lastUpdateTime) / 1000;
                if (timeDiff > 0) {
                    speed = ((distance / timeDiff) * 3.6).toFixed(1);
                }
            }
            
            const markerContent = `
                <b>Bus ID:</b> ${busId}<br>
                <b>Last Updated:</b> ${formatTime(new Date(timestamp))}<br>
                <b>Speed:</b> ${speed} km/h<br>
                <b>Location:</b> [${coordinates[1].toFixed(4)}, ${coordinates[0].toFixed(4)}]
                <br><a href="#" onclick="drawBusRoute('${busId}'); return false;">Show Route</a>
            `;
            
            if (busId in busMarkers) {
                const marker = busMarkers[busId];
                marker.setLatLng([coordinates[1], coordinates[0]]);
                marker.getPopup().setContent(markerContent);
                
                // Update route if exists
                if (marker.route) {
                    const routeCoordinates = marker.getLatLngs().map(latlng => [latlng.lat, latlng.lng]);
                    routeCoordinates.push([coordinates[1], coordinates[0]]);
                    marker.route.setLatLngs(routeCoordinates);
                }
            } else {
                console.log(`Adding new bus: ${busId}`);
                const marker = L.marker([coordinates[1], coordinates[0]])
                              .bindPopup(markerContent)
                              .addTo(mymap);
                
                // Add custom method to get lat/lng history
                marker.getLatLngs = function() {
                    if (!this._latlngs) this._latlngs = [];
                    this._latlngs.push(this.getLatLng());
                    return this._latlngs;
                };
                
                busMarkers[busId] = marker;
            }
        });
        document.getElementById('busCount').textContent = Object.keys(busMarkers).length;

        console.log(`Update complete. Active buses: ${Object.keys(busMarkers).length}`);
        lastUpdateTime = currentTime;
        
    } catch (error) {
        console.error('Error updating markers:', error);
    }
}

function setupEventSource() {
    console.log('Initializing EventSource connection...');
    var source = new EventSource('/bus-data');
    
    source.addEventListener('open', function(e) {
        console.log('EventSource connection established');
    }, false);
    
    source.addEventListener('message', function(e) {
        try {
            const geojsonData = JSON.parse(e.data);
            updateMarkers(geojsonData);
        } catch (error) {
            console.error('Error processing message:', error);
        }
    }, false);
    
    source.addEventListener('error', function(e) {
        console.error('EventSource connection lost. Retrying in 5 seconds...');
        source.close();
        setTimeout(setupEventSource, 5000);
    }, false);
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', function() {
    console.log('Initializing real-time bus tracking (10-second intervals)');
    setupEventSource();
});