/**
 * @typedef {Object} GeoJsonGeometry
 * @property {string} type
 * @property {Array<any>} coordinates
 */

/**
 * @typedef {Object} RoadProperties
 * @property {number} id
 * @property {number} [osm_id]
 * @property {string|null} [name]
 * @property {string|null} [highway]
 * @property {number} [length_m]
 */

/**
 * @typedef {Object} RoadFeature
 * @property {"Feature"} type
 * @property {GeoJsonGeometry} geometry
 * @property {RoadProperties} properties
 */

/**
 * @typedef {Object} RoadsFeatureCollection
 * @property {"FeatureCollection"} type
 * @property {RoadFeature[]} features
 */

/**
 * @typedef {Object} BBox
 * @property {number} minLon
 * @property {number} minLat
 * @property {number} maxLon
 * @property {number} maxLat
 */

export {};

