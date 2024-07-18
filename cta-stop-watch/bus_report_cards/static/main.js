const ChiLocation = [-87.63211524853163, 41.862161325588076];

dataPath = "data/bus_stops.geojson"

d3.json(dataPath, function (df) {
  sessionStorage.setItem("data", JSON.stringify(df));
});

const stops = JSON.parse(sessionStorage.getItem("data"))



var map = new maplibregl.Map({
    container: 'map',
    style: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json', // stylesheet location
    center: ChiLocation, // starting position [lng, lat]
    zoom: 11 // starting zoom
});  


map.on('load', () => {
    map.addSource('stops', {
        'type': 'geojson',
        'data': stops
    });

    map.addLayer({
        'id': 'stops',
        'type': 'circle',
        'source': 'stops',
        'layout': {},
        'paint': {
            'circle-color': '#800000',
            'circle-radius': {
                        'base': 1.75,
                        'stops': [
                            [12, 2],
                            [22, 180]
                        ]
                    },
        }
    });

    map.on('mouseenter', 'stops', (e) => {
        // Change the cursor style as a UI indicator.
        map.getCanvas().style.cursor = 'pointer';
    
        document.getElementById('report-cards').innerHTML = e.features[0].properties.public_nam;
        console.log(e)
 })
 
});