const express = require('express');
const app = express();
const fs = require('fs');
const pool = require("./db");
const csv = require('csv-parser');

app.get('/health', (req, res) => {
    res.send('ok');
});


app.get('/addGroundToDB', async (req, res, next) => {
    try {
        const clients = {
            'CleanTech': ['Hatkarwadi'],
            'HeroFutureEnergies': ['Barod', 'Ichhawar'],
            'O2Power': ['Gorai'],
            'ReNew': ['Ashoknagar', 'Bikaner-ICR10', 'Bikaner-ICR27', 'Bikaner-ICR40', 'Bikaner', 'GUVNL-ICR11', 'GUVNL-ICR6', 'GUVNL', 'Honnali', 'Kottali-ICR16', 'Kottali-ICR7', 'Kottali', 'Mandamarri', 'Nirmal', 'Pavagada', 'SECI-3', 'SECI-III-ICR15', 'SECI-III-ICR32', 'SECI-III-ICR6'],
            'Refex': ['Bhilai'],
            'Sprng': ['Arinsun', 'Kanasar', 'NPKunta', 'Pavagada'],
            'Teqo': ['Charanka', 'Goyalri', 'Rewa', 'Warrangal'],
            'Vibrant': ['Savner']
        }

        for (const [client, value] of Object.entries(clients)) {
            for (const site of value) {
                let filepath = `/home/ubuntu/efs_grounddata/clients/${client}/processed_data/${client}_Subhourly_${site}.csv`;

                const siteId = await pool.query("SELECT id FROM utility_sites WHERE sitename = $1", [site]);

                const readableStream = fs.createReadStream(filepath);

                //if filepath does not exist then skip
                if (!fs.existsSync(filepath)) {
                    console.log("File does not exist");
                    res.send("File does not exist");
                    return;
                }

                readableStream
                    .pipe(csv())
                    .on('data', async (row) => {
                        const time = row['Time'];
                        const groundGhi = parseFloat(row['Ground GHI']);
                        const groundPoa = parseFloat(row['Ground POA']);
                        const groundGen = parseFloat(row['AC_POWER_SUM']);

                        await pool.query("INSERT INTO ground_data (site_id, time, ground_ghi, ground_poa, ground_generation) VALUES ($1, $2, $3, $4, $5)", [siteId, time, groundGhi, groundPoa, groundGen]);


                    })
                    .on('end', () => {
                        res.send("GroundData added successfully to db");
                    });
            }
        }

    } catch (err) {
        console.log(err.message);
        next(err);
    }
});



app.listen(4000, () => {
    console.log('Server is up on port 4000');
});