var mysql = require('mysql');
var async = require("async");

var user_name = 'root';

var connection = mysql.createConnection({
    host     : 'localhost',
    user     : user_name,
    password : '1234',
    database : 'arsystems'
});

connection.connect();

connection.beginTransaction(function (err) {
    if (err) { throw err; }

    // Join consumer_product and product_alert which have the same prod_id
    connection.query('SELECT product_alert.product_alert_id, consumer_product.consumer_id, product_alert.prod_id from product_alert INNER JOIN consumer_product ON product_alert.prod_id = consumer_product.prod_id', function (err, matchedTrans, fields) {
        if (err) {
            console.log('Error while performing Query.\n' + err);
            throw err;
        }
        
        console.log('The solution is: ', matchedTrans);
        
        console.log(matchedTrans.length);
        
        var count = 0;
        
        // Doing sync loop to make sure manufacturer_id received before next query get executed
        async.whilst(
            function () { return count < matchedTrans.length; },
            function (callback) {
                // Find the manufacturer_id
                connection.query('SELECT product.manufacturer_id from product WHERE product.prod_id = ?', [matchedTrans[count].prod_id], function (err, mID, fields) {
                    if (err) {
                        console.log('Error while performing Query2.\n' + err);
                        throw err;
                    }
                    console.log('MID: ', mID);
                    matchedTrans[count].manufacturer_id = mID[0].manufacturer_id;
                    count++;
                    callback(err, matchedTrans);
                })
            },
            function (err, result) {
                if (err) {
                    console.log('Error in callback' + err);
                    throw err;
                } 
                console.log('The solution2222 is: ', result);
                
                count = 0;
                // Doing sync loop to make sure the transaction can commit after everything is done
                async.whilst(
                    function () { return count < result.length; },
                    function (callback) {
                        // insert new data if not exist
                        console.log('Date.now(): ' + Date.now());
                        console.log('Date.UTC(): ' + Date.UTC(2017));
                        connection.query('INSERT INTO consumer_product_alert (product_alert_id, consumer_id, prod_id, manufacturer_id, txn_userid, txn_dttm) VALUES (?, ?, ?, ?, ?, NOW()) ON DUPLICATE KEY UPDATE consumer_id=consumer_id;',
                         [result[count].product_alert_id, result[count].consumer_id, result[count].prod_id, result[count].manufacturer_id, user_name], function (err, rows, fields) {
                            if (err) {
                                console.log('Error while performing Query3.\n' + err);
                                return connection.rollback(function () {
                                    throw err;
                                });
                            }
                            //console.log('INSERT RESULT ', count, rows);
                            
                            count++;
                            callback(err, rows);
                        });
                    },
                    function (err, newResult) {
                        if (err) {
                            console.log('Error in callback' + err);
                            return connection.rollback(function () {
                                throw err;
                            });
                        }
                        //console.log('FINAL RESULT ', newResult);
                        
                        connection.commit(function (err) {
                            if (err) {
                                return connection.rollback(function () {
                                    throw err;
                                });
                            }
                            console.log('success!');
                        });
                        connection.end();
                    }
                );
            });      
    });
});
