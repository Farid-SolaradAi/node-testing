const express = require('express');
const app = express();
const fs = require('fs');
const pool = require("./db");
const csv = require('csv-parser');
const moment = require('moment');

app.get('/health', (req, res) => {
    res.send('ok');
});


app.get('/addGroundToDB', async (req, res, next) => {
    try {
        const clients = {
            // 'CleanTech': ['Hatkarwadi'],
            // 'HeroFutureEnergies': ['Barod', 'Ichhawar'],
            // 'O2Power': ['Gorai'],
            // 'ReNew': ['Ashoknagar', 'Bikaner-ICR10', 'Bikaner-ICR27', 'Bikaner-ICR40', 'GUVNL-ICR11', 'GUVNL-ICR6', 'Honnali', 'Kottali-ICR16', 'Kottali-ICR7', 'Mandamarri', 'Nirmal', 'Pavagada', 'SECI-III-ICR15', 'SECI-III-ICR32', 'SECI-III-ICR6'],
            // 'Refex': ['Bhilai'],
            // 'Sprng': ['Arinsun'],
            // 'Teqo': ['Charanka', 'Goyalri', 'Rewa', 'Warrangal'],
            // 'Vibrant': ['Savner']
        }

        for (const [client, value] of Object.entries(clients)) {
            for (const site of value) {
                let filepath = `/home/ubuntu/efs_grounddata/clients/${client}/processed-data/${client}_Subhourly_${site}.csv`;

                const siteInDB = await pool.query("SELECT id FROM utility_sites WHERE sitename = $1 AND company = $2", [site, client]);

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


app.get('/addNCMRWFToDB', async (req, res, next) => {
    try {



    }
    catch (err) {
        console.log(err.message);
        return;
    }
});


app.get('/addNcmrwfMetadata', async (req, res, next) => {
    try {
        //for 4.0k and 00Z
        const resolutions = ['NCMRWF_04P0', 'NCMRWF_12P5']
        const times = ['00Z', '12Z']

        for (const resolution of resolutions) {
            for (const time of times) {
                let directoryPath = `/home/ubuntu/efs_nwpdata/NCMRWF/processed-data/ftp.ncmrwf.gov.in/${resolution}/${time}`;

                const folders = fs.readdirSync(directoryPath)

                for (const folder of folders) {
                    let currDirPath = directoryPath + '/' + folder;
                    const files = fs.readdirSync(currDirPath);

                    for (const file of files) {
                        let filePath = currDirPath + '/' + file
                        let nwpType = resolution;

                        //find the time that file has been generated
                        const stats = fs.statSync(filePath)
                        const fileTime = moment(stats.mtime).format('YYYY-MM-DD HH:mm:ss')

                        let generatedTime = fileTime

                        //insert into nwp_metadata table
                        await pool.query("INSERT INTO nwp_metadata (nwp_type, generated_time, filepath) VALUES ($1, $2, $3)", [nwpType, generatedTime, filePath])
                        console.log(file + '    Done')
                    }
                }
            }
        }

        res.send('Done Bro, Ab chill mar')
    }
    catch (err) {
        console.log(err.message);
        return;
    }
});


const port = process.env.PORT || 3001;
const host = process.env.HOST || `localhost`;


app.listen(5001, () => {
    console.log(`Server is up on port ${5001}`);
});
