const express = require('express');
const path = require('path');
const pairRouter = require('./pair');   // the router exported by pair.js

const app = express();
const PORT = process.env.PORT || 3000;

// All routes are handled by pair.js
app.use('/', pairRouter);

app.listen(PORT, () => {
    console.log(`✅ INCONNU BOY server is live on http://localhost:${PORT}`);
});
