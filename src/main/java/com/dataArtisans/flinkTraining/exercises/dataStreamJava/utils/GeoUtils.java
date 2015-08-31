/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GeoUtils {

	// Geo boundaries of the area of NYC
	public static double LON_EAST = -73.7;
	public static double LON_WEST = -74.05;
	public static double LAT_NORTH = 41.0;
	public static double LAT_SOUTH = 40.5;

	// area width and height
	public static double LON_WIDTH = 74.05 - 73.7;
	public static double LAT_HEIGHT = 41.0 - 40.5;

	// delta step to create artificial gird overlay of NYC
	public static double DELTA_LON = 0.0014;
	public static double DELTA_LAT = 0.00125;

	// ( |LON_WEST| - |LON_EAST| ) / DELTA_LAT
	public static int NUMBER_OF_GRID_X = 250;
	// ( LAT_NORTH - LAT_SOUTH ) / DELTA_LON
	public static int NUMBER_OF_GRID_Y = 400;

	/**
	 * Checks if a location specified by longitude and latitude values is
	 * within the geo boundaries of New York City.
	 *
	 * @param lon longitude of the location to check
	 * @param lat latitude of the location to check
	 *
	 * @return true if the location is within NYC boundaries, otherwise false.
	 */
	public static boolean isInNYC(float lon, float lat) {

		if (lon > LON_EAST || lon < LON_WEST) {
			return false;
		} else if(lat > LAT_NORTH || lat < LAT_SOUTH) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * Maps a location specified by latitude and longitude values to a cell of a
	 * grid covering the area of NYC.
	 * The grid cells are roughly 100 x 100 m and sequentially number from north-west
	 * to south-east starting by zero.
	 *
	 * @param lon longitude of the location to map
	 * @param lat latitude of the location to map
	 *
	 * @return id of mapped grid cell.
	 */
	public static int mapToGridCell(float lon, float lat) {
		int xIndex = (int)Math.floor((Math.abs(LON_WEST) - Math.abs(lon)) / DELTA_LON);
		int yIndex = (int)Math.floor((LAT_NORTH - lat) / DELTA_LAT);

		return xIndex + (yIndex * NUMBER_OF_GRID_X);
	}

	/**
	 * Maps the direct path between two locations specified by longitude and latitude to a list of
	 * cells of a grid covering the area of NYC.
	 * The grid cells are roughly 100 x 100 m and sequentially number from north-west
	 * to south-east starting by zero.
	 *
	 * @param lon1 longitude of the first location
	 * @param lat1 latitude of the first location
	 * @param lon2 longitude of the second location
	 * @param lat2 latitude of the second location
	 *
	 * @return A list of cell ids
	 */
	public static List<Integer> mapToGridCellsOnWay(float lon1, float lat1, float lon2, float lat2) {

		int x1 = (int)Math.floor((Math.abs(LON_WEST) - Math.abs(lon1)) / DELTA_LON);
		int y1 = (int)Math.floor((LAT_NORTH - lat1) / DELTA_LAT);

		int x2 = (int)Math.floor((Math.abs(LON_WEST) - Math.abs(lon2)) / DELTA_LON);
		int y2 = (int)Math.floor((LAT_NORTH - lat2) / DELTA_LAT);

		int startX, startY, endX, endY;
		if(x1 <= x2) {
			startX = x1;
			startY = y1;
			endX = x2;
			endY = y2;
		}
		else {
			startX = x2;
			startY = y2;
			endX = x1;
			endY = y1;
		}

		double slope = (endY - startY) / ((endX - startX)+0.00000001);

		int curX = startX;
		int curY = startY;

		ArrayList<Integer> cellIds = new ArrayList<Integer>(64);
		cellIds.add(curX + (curY * NUMBER_OF_GRID_X));

		while(curX < endX || curY != endY) {

			if(slope > 0) {
				double y = (curX - startX + 0.5) * slope + startY - 0.5;

				if(y > curY - 0.05 && y < curY + 0.05) {
					curX++;
					curY++;
				}
				else if(y < curY) {
					curX++;
				}
				else {
					curY++;
				}
			}
			else {
				double y = (curX - startX + 0.5) * slope + startY + 0.5;

				if(y > curY - 0.05 && y < curY + 0.05) {
					curX++;
					curY--;
				}
				if(y > curY) {
					curX++;
				}
				else {
					curY--;
				}

			}

			cellIds.add(curX + (curY * NUMBER_OF_GRID_X));
		}

		return cellIds;
	}

	/**
	 * Returns the longitude of the center of a grid cell.
	 *
	 * @param gridCellId The grid cell.
	 *
	 * @return The longitude value of the cell's center.
	 */
	public static float getGridCellCenterLon(int gridCellId) {

		int xIndex = gridCellId % NUMBER_OF_GRID_X;

		float lon = (float)(Math.abs(LON_WEST) - (xIndex * DELTA_LON) - (DELTA_LON / 2)) * -1.0f;
		return lon;
	}

	/**
	 * Returns the latitude of the center of a grid cell.
	 *
	 * @param gridCellId The grid cell.
	 *
	 * @return The latitude value of the cell's center.
	 */
	public static float getGridCellCenterLat(int gridCellId) {

		int xIndex = gridCellId % NUMBER_OF_GRID_X;
		int yIndex = (gridCellId - xIndex) / NUMBER_OF_GRID_X;

		float lat = (float)(LAT_NORTH - (yIndex * DELTA_LAT) - (DELTA_LAT / 2));
		return lat;
	}

	public static float getRandomNYCLon(Random rand) {
		return (float)(LON_EAST - (LON_WIDTH * rand.nextFloat()));
	}

	public static float getRandomNYCLat(Random rand) {
		return (float)(LAT_SOUTH + (LAT_HEIGHT * rand.nextFloat()));
	}

}