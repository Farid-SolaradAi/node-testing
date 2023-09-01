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
            'ReNew': ['Ashoknagar', 'Bikaner-ICR10', 'Bikaner-ICR27', 'Bikaner-ICR40', 'GUVNL-ICR11', 'GUVNL-ICR6', 'Honnali', 'Kottali-ICR16', 'Kottali-ICR7', 'Mandamarri', 'Nirmal', 'Pavagada', 'SECI-III-ICR15', 'SECI-III-ICR32', 'SECI-III-ICR6'],
            'Refex': ['Bhilai'],
            'Sprng': ['Arinsun'],
            'Teqo': ['Charanka', 'Goyalri', 'Rewa', 'Warrangal'],
            'Vibrant': ['Savner']
        }

        for (const [client, value] of Object.entries(clients)) {
            for (const site of value) {
                let filepath = `/home/ubuntu/efs_grounddata/clients/${client}/processed-data/${client}_Subhourly_${site}.csv`;

                const siteInDB = await pool.query("SELECT id FROM utility_sites WHERE sitename = $1", [site]);

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
                        const siteId = siteInDB.rows[0].id;
                        await pool.query("INSERT INTO ground_data (site_id, time, ground_ghi, ground_poa, ground_generation) VALUES ($1, $2, $3, $4, $5)", [siteId, time, groundGhi, groundPoa, groundGen]);


                    })
                    .on('end', () => {
                        console.log(`CSV file successfully processed for ${site} and ${client}`);
                    });
            }
        }

        res.send("Done");

    } catch (err) {
        console.log(err.message);
        return;
    }
});


const port = process.env.PORT || 3001;
const host = process.env.HOST || `localhost`;


app.listen(5001, () => {
    console.log(`Server is up on port ${5001}`);
});
