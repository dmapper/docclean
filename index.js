var _ = require('lodash');
var mongo = require('mongodb');
var async = require('async');

module.exports = function(url, date, collections, callback) {
  var resultes = {};

  mongo.MongoClient.connect(url, function (err, db) {
    if (err) throw err;

    async.eachSeries(collections, cleanCollection.bind(null, db), function () {
      db.close();
      if (callback) {
        callback(null, resultes);
      } else {
        process.exit();
      }
    });

  });

  function cleanCollection(db, snapshotsCollectionName, done){
    var oplogsCollectionName1 = snapshotsCollectionName + '_ops';
    var oplogsCollectionName2 =  'ops_' + snapshotsCollectionName;

    var snapshotsCollection = db.collection(snapshotsCollectionName);
    var oplogsCollection1 = db.collection(oplogsCollectionName1);
    var oplogsCollection2 = db.collection(oplogsCollectionName2);

    snapshotsCollection.find({'_m.ctime': { $lt: date }}).toArray(function (err, snapshots) {
      if (err) throw err;

      snapshots = snapshots || [];
      var docIds = _.pluck(snapshots, '_id');

      var counter = 0;

      async.parallel([function(cb){
        snapshotsCollection.remove({_id: {$in: docIds}}, function(err, res){
          counter += res.result.n;
          cb()
        });
      }, function(cb){
        oplogsCollection1.remove({name: {$in: docIds}}, function(err, res){
          counter += res.result.n;
          cb()
        });
      }, function(cb){
        oplogsCollection2.remove({d: {$in: docIds}}, function(err, res){
          counter += res.result.n;
          cb()
        });
      }], function(){
        if (!callback) console.log(snapshotsCollectionName, counter);
        resultes[snapshotsCollectionName] = counter;
        done();
      });

    });
  }

};

