const mongoose = require('mongoose');

const cat_schema = new mongoose.Schema({
    _id: String,
    video_idx: [String],
    google_api_score: String
}, { collection: 'tedx_data_categories' });

module.exports = mongoose.model('cat', cat_schema);