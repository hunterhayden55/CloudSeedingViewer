document.addEventListener('DOMContentLoaded', function () {
    // --- Global State ---
    let map;
    let flightData = null;
    let radarOverlay = null;
    let airplaneMarker = null;
    let flightPathLayer = null;
    
    let animationState = {
        isPlaying: false,
        timer: null,
        currentIndex: 0,
        radarMeta: null,
        currentFlightId: null
    };

    // --- Initialization ---
    function initializeMap() {
        map = L.map('map').setView([38.5, -120.5], 7);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }).addTo(map);
    }

    async function populateFlightSelector() {
        try {
            const response = await fetch('processed_data/flights.json');
            const flights = await response.json();
            const selector = document.getElementById('flight-select');
            
            flights.forEach(flight => {
                const option = document.createElement('option');
                option.value = flight.id;
                option.textContent = flight.displayName;
                selector.appendChild(option);
            });
        } catch (error) {
            console.error("Could not load flights.json:", error);
            alert("Error: Could not load the list of flights.");
        }
    }

    // --- Data Loading ---
    async function loadFlight(flightId) {
        resetMap();
        animationState.currentFlightId = flightId;

        try {
            const flightResponse = await fetch(`processed_data/${flightId}/flight_data.geojson`);
            const geojsonData = await flightResponse.json();
            
            flightData = {
                path: geojsonData.features.find(f => f.geometry.type === 'LineString'),
                points: geojsonData.features.filter(f => f.geometry.type === 'Point')
            };

            const metaResponse = await fetch(`processed_data/${flightId}/radar_meta.json`);
            animationState.radarMeta = await metaResponse.json();

            drawFlightPath();
            setupAnimation();
            
            document.getElementById('animation-controls').classList.remove('hidden');

        } catch (error) {
            console.error(`Error loading data for flight ${flightId}:`, error);
            alert(`Could not load data for this flight. Make sure flight_data.geojson and radar_meta.json exist.`);
        }
    }

    // --- Map Drawing & Animation Setup ---
    function drawFlightPath() {
        if (flightData.path) {
            flightPathLayer = L.geoJSON(flightData.path, { style: { color: '#333', weight: 2, opacity: 0.7 } }).addTo(map);
            map.fitBounds(flightPathLayer.getBounds().pad(0.1));
        }
    }

    function setupAnimation() {
        const startPoint = flightData.points[0].geometry.coordinates;
        airplaneMarker = L.circleMarker([startPoint[1], startPoint[0]], { radius: 6, color: 'black', weight: 1, fillOpacity: 1.0 }).addTo(map);

        const radarImageUrl = `processed_data/${animationState.currentFlightId}/radar_frames/frame_0.png`;
        radarOverlay = L.imageOverlay(radarImageUrl, animationState.radarMeta.bounds).addTo(map);

        const slider = document.getElementById('timeline-slider');
        slider.max = flightData.points.length - 1;
        slider.value = 0;
        slider.disabled = false;
        
        updateFrame(0);
    }
    
    // --- Animation Logic ---
    function playAnimation() {
        if (animationState.isPlaying) return;
        animationState.isPlaying = true;
        document.getElementById('play-pause-btn').textContent = 'Pause';
        document.getElementById('play-pause-btn').classList.add('playing');

        animationState.timer = setInterval(() => {
            let nextIndex = animationState.currentIndex + 1;
            if (nextIndex >= flightData.points.length) {
                nextIndex = 0; // Loop animation
            }
            updateFrame(nextIndex);
        }, 50); // Faster update for smoother plane movement (20fps)
    }

    function pauseAnimation() {
        if (!animationState.isPlaying) return;
        animationState.isPlaying = false;
        document.getElementById('play-pause-btn').textContent = 'Play';
        document.getElementById('play-pause-btn').classList.remove('playing');
        clearInterval(animationState.timer);
    }

    function updateFrame(pointIndex) {
        animationState.currentIndex = pointIndex;
        const point = flightData.points[pointIndex];
        if (!point) return;

        // Find the correct radar frame for the current point's timestamp
        const pointTime = new Date(point.properties.timestamp_iso);
        let radarFrameIndex = 0;
        for (let i = animationState.radarMeta.timestamps.length - 1; i >= 0; i--) {
            const radarTime = new Date(animationState.radarMeta.timestamps[i]);
            if (pointTime >= radarTime) {
                radarFrameIndex = i;
                break;
            }
        }
        
        const newImageUrl = `processed_data/${animationState.currentFlightId}/radar_frames/frame_${radarFrameIndex}.png`;
        if (radarOverlay.getUrl() !== newImageUrl) {
            radarOverlay.setUrl(newImageUrl);
        }

        // Update airplane position
        const coords = point.geometry.coordinates;
        airplaneMarker.setLatLng([coords[1], coords[0]]);
        
        // Update marker color based on seeding type
        const type = point.properties.seeding_type;
        let color = '#ff7800'; // Default: orange
        if (type === 'BIP') color = '#ff00ff'; // Magenta
        else if (type === 'Eject') color = '#ffff00'; // Yellow
        else if (type === 'Generator') color = '#00ffff'; // Cyan
        airplaneMarker.setStyle({ fillColor: color });

        // Update UI
        document.getElementById('timeline-slider').value = pointIndex;
        document.getElementById('timestamp-display').textContent = pointTime.toLocaleTimeString();
    }
    
    function resetMap() {
        pauseAnimation();
        if (flightPathLayer) map.removeLayer(flightPathLayer);
        if (airplaneMarker) map.removeLayer(airplaneMarker);
        if (radarOverlay) map.removeLayer(radarOverlay);
        flightData = null;
        animationState.currentIndex = 0;
        document.getElementById('animation-controls').classList.add('hidden');
        document.getElementById('timeline-slider').disabled = true;
    }

    // --- Event Listeners ---
    document.getElementById('flight-select').addEventListener('change', (e) => e.target.value ? loadFlight(e.target.value) : resetMap());
    document.getElementById('play-pause-btn').addEventListener('click', () => animationState.isPlaying ? pauseAnimation() : playAnimation());
    document.getElementById('timeline-slider').addEventListener('input', (e) => {
        pauseAnimation();
        updateFrame(parseInt(e.target.value, 10));
    });

    // --- Start the App ---
    initializeMap();
    populateFlightSelector();
});