// Import packages
import React, { useState } from 'react';
import DeckGL, { ScatterplotLayer } from 'deck.gl';
import { StaticMap } from 'react-map-gl';

// Create config file 
import config from '../config';

// Import data 
import data from './trip_data.json' assert { type: 'json' };

// Set up map 
const canvasStyle = {
    position: "relative",
    width: "100%",
    height: "600px",
    border: "1px solid black",
  };
  
const mapStyle = 'mapbox://styles/mapbox/light-v9';
const mapboxApiAccessToken = config('mapboxApiAccessToken');

  // Viewport settings: Center in Chicago
const initialViewState = {
    longitude: -87.6298,
    latitude: 41.8781,
    zoom: 11,
    minZoom: 5,
    maxZoom: 16,
    pitch: 45,
    bearing: 0,
  };
  
const DeckGLBasicMap = () => {
    const [viewState, setViewState] = useState(initialViewState);
  
    const layers = [
      new ScatterplotLayer({
        id: 'scatterplot',
        // data: '/data/taxi.json', // load data from server
        data: data, 
        getPosition: d => d.position,
        getColor: d => [0, 188, 255],
        getRadius: d => 25,
        opacity: 0.9,
        pickable: false,
        radiusMinPixels: 0.25,
        radiusMaxPixels: 30,
      }),
    ];
  
    return (
      <DeckGL
        viewState={viewState}
        layers={layers}
        style={canvasStyle}
        getCursor={() => 'default'}
        onViewStateChange={
          (nextViewState) => {
            setViewState(nextViewState.viewState);
          }
        }
        controller
      >
        <StaticMap mapboxApiAccessToken={mapboxApiAccessToken} mapStyle={mapStyle} />
      </DeckGL>
    );
  }
  
  export default DeckGLBasicMap;