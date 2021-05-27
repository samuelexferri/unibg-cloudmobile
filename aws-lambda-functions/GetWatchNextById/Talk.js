const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    _id: String,
    title: String,
    url: String,
    details: String,
    main_author: String,
    num_views: String,
    watch_next_s:  [String]
}, { collection: 'tedx_data_total' });

module.exports = mongoose.model('talk', talk_schema);