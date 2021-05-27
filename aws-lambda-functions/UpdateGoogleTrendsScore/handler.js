const connect_to_db = require('./db');

const googleTrends = require('google-trends-api');

// GET BY TALK HANDLER

const cat = require('./Cat');

 module.exports.update_google_score =  (event, context, callback) =>  {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));

    connect_to_db().then(() => {
        
        // Boolean, if `google-trens-api` is working (true) or not (false)
        var isGoogleTrendsAPIWorking = false // Deprecated package

        async function f() {
            for await (const c of cat.find()) {
                console.log(c)
                
                var key = c._id

                if (isGoogleTrendsAPIWorking) {
                    // Get the categories' score from 0 to 100
                    googleTrends.interestOverTime({keyword: key, startTime: new Date(Date.now() - 1)}).then(function(results){
                      console.log(results)
                      
                      // Score from Google Trends API
                      var score = results
                    })
                    .catch(function(err){
                      console.error(err)
                    });
                } else {
                    // Score random
                    var score = Math.floor((Math.random() * 100) + 1);
                }
                
                await cat.updateOne({_id: key}, {google_api_score: score});
            }
        }

        f()
        
        cat.find().then(t => {callback(null, {
            statusCode: 200,
            body: JSON.stringify("DB Aggiornato")
        })})
    });
};