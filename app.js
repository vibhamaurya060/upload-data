const express = require('express');
const fs = require('fs');
const readline = require('readline');
const { MongoClient } = require('mongodb');
const bodyParser = require('body-parser');

// MongoDB connection URI
const uri = 'mongodb://localhost:27017';
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

const app = express();
const port = 3000;

let logs = [];

// Middleware
app.use(bodyParser.json());
app.use(express.static('public'));

// Connect to MongoDB
async function connectToDatabase() {
    try {
        await client.connect();
        console.log('Connected to MongoDB');
    } catch (err) {
        console.error('Error connecting to MongoDB:', err);
    }
}

// Function to parse and insert data into MongoDB
async function insertData(data) {
    const db = client.db('sample_database');
    const collection = db.collection('sample_collection');
    try {
        await collection.insertMany(data);
        logs.push({ type: 'success', message: 'Data inserted into MongoDB' });
    } catch (err) {
        logs.push({ type: 'error', message: 'Error inserting data into MongoDB: ' + err });
    }
}

// Function to read data from file and update MongoDB
async function processDataFile() {
    const fileStream = fs.createReadStream('data.json');
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let newData = [];

    for await (const line of rl) {
        try {
            const entry = JSON.parse(line);
            newData.push(entry);
        } catch (err) {
            logs.push({ type: 'error', message: 'Error parsing JSON: ' + err });
        }
    }

    if (newData.length > 0) {
        await insertData(newData);
    }
}

// Function to filter logs based on type
function filterLogs(type) {
    return logs.filter(log => log.type === type);
}

// Endpoint to fetch logs
app.get('/logs', (req, res) => {
    const type = req.query.type;
    if (type) {
        res.json(filterLogs(type));
    } else {
        res.json(logs);
    }
});

// Endpoint to post data
app.post('/data', async (req, res) => {
    const data = req.body;
    try {
        await insertData([data]);
        res.status(200).json({ message: 'Data inserted successfully' });
    } catch (err) {
        res.status(500).json({ message: 'Error inserting data', error: err });
    }
});

// Serve the HTML page
app.get('/', (req, res) => {
    res.sendFile(__dirname + '/public/index.html');
});

// Start the server
app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});

// Initial setup
connectToDatabase()
    .then(() => {
        // Run data processing script twice a day
        setInterval(() => {
            console.log('\n----- Processing Data File -----');
            processDataFile();
        }, 12 * 60 * 60 * 1000); // Run every 12 hours
    })
    .catch(err => console.error('Error connecting to database:', err));
