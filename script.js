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
        currentFlightId: null,
        currentRadarFile: null
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

    // --- DATA LOADING (Reverted to simple version) ---
    async function loadFlight(flightId) {
        resetMap();
        animationState.currentFlightId = flightId;

        try {
            // Fetch flight data
            const flightResponse = await fetch(`processed_data/${flightId}/flight_data.geojson`);
            const geojsonData = await flightResponse.json();
            
            flightData = {
                path: geojsonData.features.find(f => f.geometry.type === 'LineString'),
                points: geojsonData.features.filter(f => f.geometry.type === 'Point')
            };

            // Fetch the single, complete radar metadata file
            const metaResponse = await fetch(`processed_data/${flightId}/radar_meta.json`);
            animationState.radarMeta = await metaResponse.json();
            // Sorting is still good practice, though the Python script should handle it
            animationState.radarMeta.frames.sort((a, b) => new Date(a.time) - new Date(b.time));

            drawFlightPath();
            setupAnimation();
            
            document.getElementById('animation-controls').classList.remove('hidden');

        } catch (error) {
            console.error(`Error loading data for flight ${flightId}:`, error);
            alert(`Could not load data for this flight. Make sure flight_data.geojson and radar_meta.json exist and are correctly formatted.`);
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
        airplaneMarker = L.circleMarker([startPoint[1], start_point[0]], { radius: 6, color: 'black', weight: 1, fillOpacity: 1.0 }).addTo(map);

        const firstFrameFile = animationState.radarMeta.frames[0].file;
        const radarImageUrl = `processed_data/${animationState.currentFlightId}/radar_frames/${firstFrameFile}`;
        radarOverlay = L.imageOverlay(radarImageUrl, animationState.radarMeta.bounds).addTo(map);
        animationState.currentRadarFile = firstFrameFile;

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
        }, 50);
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

        const pointTime = new Date(point.properties.timestamp_iso);
        let correctFrame = animationState.radarMeta.frames[0];
        for (const frame of animationState.radarMeta.frames) {
            if (new Date(frame.time) <= pointTime) {
                correctFrame = frame;
            } else {
                break;
            }
        }
        
        if (animationState.currentRadarFile !== correctFrame.file) {
            const newImageUrl = `processed_data/${animationState.currentFlightId}/radar_frames/${correctFrame.file}`;
            radarOverlay.setUrl(newImageUrl);
            animationState.currentRadarFile = correctFrame.file;
        }

        const coords = point.geometry.coordinates;
        airplaneMarker.setLatLng([coords[1], coords[0]]);
        
        const type = point.properties.seeding_type;
        let color = '#ff7800';
        if (type === 'BIP') color = '#ff00ff';
        else if (type === 'Eject') color = '#ffff00';
        else if (type === 'Generator') color = '#00ffff';
        airplaneMarker.setStyle({ fillColor: color });

        document.getElementById('timeline-slider').value = pointIndex;
        
        const pstTime = pointTime.toLocaleTimeString('en-US', { timeZone: 'America/Los_Angeles' });
        const utcTime = pointTime.toLocaleTimeString('en-GB', { timeZone: 'UTC' });
        document.getElementById('timestamp-display').textContent = `${pstTime} PST | ${utcTime} UTC`;
    }
    
    function resetMap() {
        pauseAnimation();
        if (flightPathLayer) map.removeLayer(flightPathLayer);
        if (airplaneMarker) map.removeLayer(airplaneMarker);
        if (radarOverlay) map.removeLayer(radarOverlay);
        flightData = null;
        animationState.currentIndex = 0;
        animationState.currentRadarFile = null;
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