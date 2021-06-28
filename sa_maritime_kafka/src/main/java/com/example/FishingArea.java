package com.example;

import java.io.File;
import java.io.IOException;
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
    
    private File file;
    private SimpleFeatureCollection collection;

    public FishingArea(){
        this.file = new File("/home/mivia/Desktop/ais_data/v_recode_fish_area_clean.shp");
        Map<String, String> connect = new HashMap<String, String>();
        connect.put("url", file.toURI().toString());
        
        String[] typeNames;
        try {
            DataStore dataStore = DataStoreFinder.getDataStore(connect);
            typeNames = dataStore.getTypeNames();
            String typeName = typeNames[0];
            System.out.println("Reading content : " + typeName);
            SimpleFeatureSource featureSource = dataStore.getFeatureSource(typeName);
            this.collection = featureSource.getFeatures();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

    public boolean is_in_FishingArea(String v){
        final String[] data = v.split(",");
        String Longitude = data[4];
        String Latitude = data[3];

        SimpleFeatureIterator iterator = this.collection.features();
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
        boolean is_in = false;
        try {
            while (iterator.hasNext()) {
                SimpleFeature feature = iterator.next();
                Geometry polygon= (Geometry)feature.getDefaultGeometry();
                org.locationtech.jts.geom.Point point = geometryFactory.createPoint(new Coordinate(Double.parseDouble(Longitude),Double.parseDouble(Latitude)));
                if(polygon.contains(point)){
                    System.out.println("sono nell'area");
                    return true;
                }else{

                }
            }
        }catch (Throwable e) {
            
        } finally {

            iterator.close();
        }
        return is_in;
    }

}
