const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    title: String,
    url: String,
    details: String,
    main_author: String
}, { collection: 'tedx_data_total' });

module.exports = mongoose.model('talk', talk_schema);