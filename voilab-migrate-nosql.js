/*jslint node: true, unparam: true, nomen: true */
(function () {
    'use strict';

        /**
         * Public service
         *
         * @param {Bucket} appBucket A couchbase bucket
         * @param {Object} [migrateConfig] A configuration object looking like this one:
         *                  {
         *                      typefield: "type",                 // fieldname in database documents that holds the document type
         *                      versionfield: "version",           // fieldname in database documents that holds the version number
         *                      filenameSeparator: "::",           // usually in a noSQL database, you name your documents using some separator
         *                      migrations_path: "../migrations/", // path to migrations files from that library
         *                      parallelLimit: 8,                  // number of parallel upgrades in batch mode
         *                      versions: {                        // optional: define here the up-to-date version number for each
         *                          article: 1,                    //           document type. You can also omit this config and
         *                          comment: 3                     //           manage your version numbers directly in the database
         *                      }                                  //           in files named like [typefield][filenameSeparator][versionfield] and
         *                  }                                      //           containing this kind of object: {"current": 3}
         *
         * @returns {{getCurrentVersion: service.getCurrentVersion, upgrade: service.upgrade}}
         */
        var migrate = function (appBucket, migrateConfig) {
            var lodash = require('lodash'),
                async = require('async'),

                /**
                 * Some private stuffs
                 */
                config = lodash.merge({
                    typefield: "type",
                    versionfield: "version",
                    filenameSeparator: "::",
                    migrations_path: "../migrations/",
                    parallelLimit: 8
                }, (migrateConfig || {})),
                versions = {},
                migrations = {},
                dbGetCurrentVersion = function (type, callback) {
                    appBucket.get(type + config.filenameSeparator + config.versionfield, function (err, doc) {
                        var version = 0;
                        if (!err) {
                            version = doc.value.current;
                        }

                        versions[type] = version;

                        return callback(null, version);
                    });
                },
                saveDocument = function (record, documentId, callback) {
                    appBucket.upsert(documentId, record, callback);
                },

                /**
                 * Method that actually pass your record through a migration function
                 *
                 * Important notice: Be aware that the migrations functions are organized within a directory
                 * that must contain subdirectories named after the record's types, each of
                 * them containing files named after the migrations version number (1.js, 2.js, 3.js, etc.)
                 *
                 * @param {Integer} version The migration version to call
                 * @param {Object} record The record to upgrade
                 * @param {String} documentId The record's document name
                 * @param callback
                 * @returns {*}
                 */
                passMigration = function (version, record, documentId, callback) {
                    // caching migration functions
                    if (!lodash.isFunction(migrations[version])) {
                        migrations[version] = require(config.migrations_path + record[config.typefield] + '/' + version);
                    }

                    // migration function is called asynchronously
                    // Thus, it must pass the resulting record in a callback.
                    return migrations[version](record, function (err, newRecord) {
                        if (err) {
                            return callback(err);
                        }

                        saveDocument(newRecord, documentId, function (err) {
                            if (err) {
                                return callback(err);
                            }

                            callback(null, newRecord[config.versionfield], newRecord, documentId);
                        });
                    });
                };

            var service = {

                /**
                 * Retrieve current (i.e. most up-to-date) version number for a record
                 *
                 * @param {Object} record
                 * @param {Function} callback
                 * @returns {*}
                 */
                getCurrentVersion: function (record, callback) {
                    // if last versions numbers are defined directly in the configuration
                    // the lib won't try to fetch them in the database.
                    if (config.versions !== undefined) {
                        return callback(null, (config.versions[record[config.typefield]] || 0));
                    }

                    // here we are in a 'versions count in database' mode. So we check our cache
                    // first, and then, we try to get the version in the database and save it in the cache.
                    if (versions[record[config.typefield]] !== undefined) {
                        return callback(null, versions[record[config.typefield]]);
                    }
                    dbGetCurrentVersion(record[config.typefield], callback);
                },

                /**
                 * Main method. Upgrade one record.
                 *
                 * @param {Object} record The record to upgrade to the latest version
                 * @param {String} documentId The document name (useful if you want to update your document :P)
                 * @param {Function} callback The callback will be called with the upgraded record.
                 */
                upgrade: function (record, documentId, callback) {
                    service.getCurrentVersion(record, function (err, lastVersion) {
                        var docVersion = record[config.versionfield] || 0,
                            initialDocVersion = docVersion,
                            i,
                            tasks = [];

                        // hint: getCurrentVersion cannot raise any error. Worst case, it returns 0.

                        // in that case, there surely is absolutely nothing to do with that record...
                        if (lastVersion === 0) {
                            return callback(null, record);
                        }

                        // now if our record version (docVersion) is smaller than the up-to-date version,
                        // we have to upgrade the record.
                        if (docVersion < lastVersion) {
                            docVersion += 1;
                            for (i = docVersion; i <= lastVersion; i += 1) {
                                // first pass
                                if (i === docVersion) {
                                    tasks.push(function (callback) {
                                        passMigration(docVersion, record, documentId, callback);
                                    });

                                    // subsequents passes
                                } else {
                                    tasks.push(passMigration);
                                }
                            }

                            // async.waterfall ensure that each migration function is over, before the next migration occurs.
                            // the next migration takes the record returned by the preceding migration as an argument.
                            async.waterfall(tasks, function (err, version, record) {
                                if (err) {
                                    return callback(err);
                                }
                                callback(null, record, (lastVersion - initialDocVersion));
                            });

                            // our record is up-to-date, let's return it.
                        } else {
                            return callback(null, record, 0);
                        }
                    });
                },

                /**
                 * Batch mode. Upgrade all records returned by fetcher function.
                 *
                 * @param {Function} fetcher An asynchronous function that provide a callback with a View result.
                 * @param callback
                 */
                batch: function (fetcher, callback) {
                    fetcher(function (err, docs) {
                        var tasks = lodash.map(docs, function (doc) {
                            return function (callback) {

                                // have to surround in a setTimeout block, because if there is no upgrade to do,
                                // upgrade function is not asynchronous and if there is many successives synchronous calls
                                // in an asynchronous context, like async.parallel, node is likely to crash with a stack overflow...
                                setTimeout(function () {
                                    service.upgrade(doc.doc, doc.id, function (err, record, nbMigrations) {
                                        if (err) {
                                            return callback(err);
                                        }

                                        callback(null, {
                                            record: record,
                                            nbMigrations: nbMigrations
                                        });
                                    });
                                }, 0);
                            };
                        });
                        async.parallelLimit(tasks, config.parallelLimit, function (err, result) {
                            if (err) {
                                return callback(err);
                            }

                            callback(null, {
                                handled: result.length,
                                upgraded: lodash.filter(result, function (r) { return r.nbMigrations > 0 }).length,
                                total: lodash.reduce(result, function (sum, r) { return (sum + r.nbMigrations); }, 0)
                            });
                        });
                    })
                }
            };

            return service;
        };

    module.exports = migrate;
}());