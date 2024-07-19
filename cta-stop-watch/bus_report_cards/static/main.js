
//map.setLayoutProperty(layer, 'visibility', 'none')
const ChiLocation = [-87.63211524853163, 41.862161325588076];
const layers = ['stop', 'route', 'ca']

stopsPath = "data/bus_stops.geojson"
routesPath = "data/routes.geojson"
caPath = "data/ca_boundaries.geojson"

var map = new maplibregl.Map({
    container: 'map',
    style: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json', // stylesheet location
    center: ChiLocation, // starting position [lng, lat]
    zoom: 11 // starting zoom
});  


map.on('load', () => {
    map.addSource('stops', {
        'type': 'geojson',
        'data': stopsPath
    });

    map.addSource('routes', {
        'type': 'geojson',
        'data': routesPath
    });

    map.addSource('cas', {
        'type': 'geojson',
        'data': caPath
    });

    map.addLayer({
        'id': 'stop',
        'type': 'circle',
        'source': 'stops',
        'layout': {
            'visibility': 'visible'
        },
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

    map.addLayer({
        'id': 'route',
        'type': 'line',
        'source': 'routes',
        'layout': {
            'line-cap': 'round',
            'visibility': 'none'},
        'paint': {
            'line-color': 'blue',
            'line-width': {
                'base': 2,
                'stops': [
                    [12, 2],
                    [22, 10]
                ]
            },
        }
    });

    map.addLayer({
        'id': 'ca',
        'type': 'fill',
        'source': 'cas',
        'layout': {
            'visibility': 'none'},
        'paint': {
            'fill-color': 'green',
            'fill-outline-color': "white",
            'fill-opacity': 0.5
        }
    });

    map.on('mouseenter', 'stop', (e) => {
        // Change the cursor style as a UI indicator.
        map.getCanvas().style.cursor = 'pointer';
    
        document.getElementById('report-cards').innerHTML = e.features[0].properties.public_nam;
        //console.log(e)
    })
    map.on('mouseenter', 'route', (e) => {
        // Change the cursor style as a UI indicator.
        map.getCanvas().style.cursor = 'pointer';
    
        document.getElementById('report-cards').innerHTML = e.features[0].properties.ROUTE;
        //console.log(e)
    })

    map.on('mousemove', 'ca', (e) => {
        // Change the cursor style as a UI indicator.
        map.getCanvas().style.cursor = 'pointer';
    
        document.getElementById('report-cards').innerHTML = e.features[0].properties.community;
    })

 map.on('click', 'stop', (e) => {
    // Change the cursor style as a UI indicator.
    map.getCanvas().style.cursor = 'pointer';

    document.getElementById('report-cards').innerHTML = e.features[0].properties.public_nam + "<br />" + "Services route(s) " + e.features[0].properties.routesstpg;
})
 
});




function showLayer(layer) {

    map.setLayoutProperty(layer, 'visibility', 'visible')
    toRemove = layers.filter(l => l !== layer);

    for (l of toRemove) {
            map.setLayoutProperty(l, 'visibility', 'none')
        }
    }