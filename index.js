var _ = require('lodash');
var mongo = require('mongodb');
var async = require('async');
var {parse} = require('url');

module.exports = function(url, date, collections, callback) {
  var resultes = {};
  var parsedUrl = parse(url)
  var dbName = parsedUrl.pathname.slice(1)

  mongo.MongoClient.connect(url, { useNewUrlParser: true }, function (err, client) {
    var db = client.db(dbName)
    if (err) throw err;

    async.eachSeries(collections, cleanCollection.bind(null, db), function () {
      client.close();
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
    var oplogsCollectionName3 =  'o_' + snapshotsCollectionName;

    var snapshotsCollection = db.collection(snapshotsCollectionName);
    var oplogsCollection1 = db.collection(oplogsCollectionName1);
    var oplogsCollection2 = db.collection(oplogsCollectionName2);
    var oplogsCollection3 = db.collection(oplogsCollectionName3);

    var counter = 0;
    async.forever(function(next){

      snapshotsCollection.find({'_m.ctime': { $lt: date }}).project({_id:1}).limit(1000).toArray(function (err, snapshots) {
        if (err) throw err;

        snapshots = snapshots || [];
        var docIds = _.pluck(snapshots, '_id');

        if (docIds.length === 0) {
          return next('done');
        }

        async.parallel([function(cb){
          snapshotsCollection.remove({_id: {$in: docIds}}, function(err, res){
            if (err) {
              console.log('Snap - error', err);
              return cb()
            }
            counter += res.result.n;
            cb()
          });
        }, function(cb){
          oplogsCollection1.remove({name: {$in: docIds}}, function(err, res){
            if (err) {
              console.log('Ops1 - error', err);
              return cb()
            }
            counter += res.result.n;
            cb()
          });
        }, function(cb){
          oplogsCollection2.remove({d: {$in: docIds}}, function(err, res){
            if (err) {
              console.log('Ops2 - error', err);
              return cb()
            }

            counter += res.result.n;
            cb()
          });
        }, function(cb){
          oplogsCollection3.remove({d: {$in: docIds}}, function(err, res){
            if (err) {
              console.log('Ops3 - error', err);
              return cb()
            }

            counter += res.result.n;
            cb()
          });
        }], function(){
          if (!callback) console.log(snapshotsCollectionName, counter);
          next();
        });

      });

    }, function(err){
      resultes[snapshotsCollectionName] = counter;
      done()
    });
  }

};

