
//map.setLayoutProperty(layer, 'visibility', 'none')
const ChiLocation = [-87.63211524853163, 41.862161325588076];
const layers = ['stop', 'route', 'ca']
const layerType = {stop: 'circle', route: 'line', ca: 'fill'}

let xwalk = []

fetch('data/community_stops.geojson').then(response => response.json())
                                     .then(data => {
                                        xwalk = data
                                    console.log(xwalk)}
                                     )

stopsPath = "data/bus_stops.geojson"
routesPath = "data/routes.geojson"
caPath = "data/ca_boundaries.geojson"

var map = new maplibregl.Map({
    container: 'map',
    style: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json', // stylesheet location
    center: ChiLocation, // starting position [lng, lat]
    zoom: 9.5 // starting zoom
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
            'visibility': 'none'
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
            'visibility': 'visible'},
        'paint': {
            'fill-color': 'green',
            'fill-outline-color': "white",
            'fill-opacity': 0.5
        }
    });

    map.on('mouseenter', 'stop', (e) => {
        // Change the cursor style as a UI indicator.
        map.getCanvas().style.cursor = 'pointer';
        const stop = e.features[0].properties.systemstop;
        map.setPaintProperty('stop', 'circle-radius', ['case',['==', ['get','systemstop'], stop], 6, 2]);


        document.getElementById('report-cards').innerHTML = e.features[0].properties.public_nam;
      
    })

    // map.on('mouseleave', 'stop', (e) => {
    //     // Change the cursor style as a UI indicator.
    //     map.getCanvas().style.cursor = 'pointer';
    //     console.log('leave')
    //     map.setPaintProperty('stop', 'circle-radius',  2);      
    // })


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

    // find the community area that the stop is in
    const stop = parseInt(parseFloat(e.features[0].properties.systemstop));
    const ca = xwalk.features.filter(x => x.properties.stpid == stop)[0].properties.community;

    const all_stops = xwalk.features.filter(x => x.properties.community == ca).map(x => x.properties.stpid);

    // show only those stops in the community area
    map.setPaintProperty('stop', 'circle-opacity', ['case',['==', ['get','systemstop'], String(stop)+'.0'], 1, 0]);


    // show only community area
    map.setLayoutProperty('ca', 'visibility', 'visible')
    map.setPaintProperty('ca', 'fill-opacity', ['case',['==', ['get','community'], ca], .5, 0]);

    document.getElementById('report-cards').innerHTML = e.features[0].properties.public_nam + "<br />" + "Services route(s) " + e.features[0].properties.routesstpg;
})

map.on('click', 'ca', (e) => {
    // Change the cursor style as a UI indicator.
    map.getCanvas().style.cursor = 'pointer';

    // find the community area that the stop is in
    const ca = e.features[0].properties.community;
    const all_stops = xwalk.features.filter(x => x.properties.community == ca).map(x => x.properties.stpid);

    // show only those stops in the community area
    const filter = [
        "case",
        ['any',
        ['in', ['get', 'systemstop'],["literal", all_stops.map(x => String(x)+'.0')]],
        ],
        1,
        0
      ]

    //const filter = ['match',['get', 'systemstop'],...all_stops]
    map.setLayoutProperty('stop', 'visibility', 'visible')
    map.setPaintProperty('stop', 'circle-opacity', filter);

    // show only community area
    map.setPaintProperty('ca', 'fill-opacity', ['case',['==', ['get','community'], ca], .5, 0]);
})

map.on('click', 'stop', (e) => {
    // Change the cursor style as a UI indicator.
    map.getCanvas().style.cursor = 'pointer';

    // find the community area that the stop is in
    const stop = e.features[0].properties.systemstop;
    const rts  = e.features[0].properties.routesstpg.split(',');

    // hide all other bus stops
    map.setPaintProperty('stop', 'circle-opacity', ['case', ['==', ['get','systemstop'],  String(stop)], 1, 0]);

    // show only those stops in the community area
    const filter = [
        "case",
        ['any',
        ['in', ['get', 'ROUTE'],["literal", rts]],
        ],
        1,
        0
      ];

    //const filter = ['match',['get', 'systemstop'],...all_stops]
    map.setLayoutProperty('route', 'visibility', 'visible');
    map.setPaintProperty('route', 'line-opacity', filter);

    // hide only community area
    map.setLayoutProperty('ca', 'visibility', 'none')});

    
 
});




function showLayer(layer) {

    map.setLayoutProperty(layer, 'visibility', 'visible')
    // bring all stuff back
    map.setPaintProperty(layer, layerType[layer] + '-opacity', .5);

    toRemove = layers.filter(l => l !== layer);

    for (l of toRemove) {
            map.setLayoutProperty(l, 'visibility', 'none')
        }
    }




function findStopsInCA(ca) {
    return xwalk.features.filter(x => x.properties.community == ca).map(x => x.properties.stpid);
}

function findRoutesForStop(ca) {
    return xwalk.features.filter(x => x.properties.community == ca).map(x => x.properties.routesstpg);
}

function findStopsForRoute(route) {
    return xwalk.features.filter(x => x.properties.routesstpg.includes(route)).map(x => x.properties.stpid);
}