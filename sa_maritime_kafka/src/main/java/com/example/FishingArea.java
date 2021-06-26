package com.example;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.JTSFactoryFinder;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import org.opengis.feature.simple.SimpleFeature;



public class FishingArea {
    
    public static void main(String[] args) {

        File file = new File("/home/mivia/Desktop/ais_data/v_recode_fish_area_clean.shp");

        try {
            Map<String, String> connect = new HashMap<String, String>();
            connect.put("url", file.toURI().toString());

            DataStore dataStore = DataStoreFinder.getDataStore(connect);
            String[] typeNames = dataStore.getTypeNames();
            String typeName = typeNames[0];

            System.out.println("Reading content : " + typeName);

            SimpleFeatureSource featureSource = dataStore.getFeatureSource(typeName);
            SimpleFeatureCollection collection = featureSource.getFeatures();
            SimpleFeatureIterator iterator = collection.features();
            GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
            try {
                while (iterator.hasNext()) {

                    SimpleFeature feature = iterator.next();
                    Geometry polygon= (Geometry)feature.getDefaultGeometry();
                    org.locationtech.jts.geom.Point point = geometryFactory.createPoint(new Coordinate(Double.parseDouble("-5.974977705924165"),Double.parseDouble("48.53263718443590")));
                    if(polygon.contains(point))
                        System.out.println("tutt appost");
                    else{

                    }
                }
            } finally {
                iterator.close();
            }

        } catch (Throwable e) {
            
        }

    }
}
