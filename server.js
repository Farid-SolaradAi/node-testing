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
                        const fileTime = moment(stats.mtime).format('YYYY-MM-DD HH:mm:ss.SSS')

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


app.get('/addNcmrwfData', async (req, res, next) => {
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
                        const parts = file.split('_');
                        const siteName = parts[0];
                        const companyName = parts[1];

                        //get the site id from utility_sites table
                        const siteInDB = await pool.query("SELECT id FROM utility_sites WHERE sitename = $1 AND company = $2", [siteName, companyName]);

                        //check if site exists in utility_sites table
                        if (siteInDB.rows.length === 0) {
                            console.log(`Site ${siteName} of ${companyName} does not exist in utility_sites table`);
                            // res.send(`Site ${siteName} of ${companyName} does not exist in utility_sites table`);
                            return;
                        }

                        const siteId = siteInDB.rows[0].id;
                        //find the time that file has been generated
                        const stats = fs.statSync(filePath)
                        const fileTime = moment(stats.mtime).format('YYYY-MM-DD HH:mm:ss.SSS')

                        let generatedTime = fileTime

                        const readableStream = fs.createReadStream(filePath)

                        readableStream
                            .pipe(csv())
                            .on('data', async (row) => {
                                const ncmrwf_type = resolution;
                                const type = time;
                                const generated_time = generatedTime;
                                const site_id = siteId;
                                const rowTime = row['TIME'] || null
                                const ghi = row['SHORTWAVE_DOWNWARDS(W/m2)'] || null
                                const precipitation = row['PRECIPITATION(mm)'] || null;
                                const surface_pressure = row['SURF_PRES(Pa)'] || null;
                                const sealevel_pressure = row['MEAN_SEALEV_PRES(Pa)'] || null;
                                const humidity = row['SPHUMIDITY_2M(Kg/Kg)'] || null;
                                const temp_2m = row['AIRTEMP_2M(degC)'] || null;
                                const low_cloud_cover_perc = row['LOW_CLOUD_COVER(Percentage)'] || null;
                                const wind_speed_10m = row['WINDSPEED_10M(m/s)'] || null
                                const wind_speed_50m = row['WINDSPEED_50M(m/s)'] || null

                                await pool.query("INSERT INTO ncmrwf_data (ncmrwf_type, type, generated_time, site_id, time, ghi, precipitation, surface_pressure, sealevel_pressure, humidity, temp_2m, low_cloud_cover_perc, wind_speed_10m, wind_speed_50m) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10 , $11, $12, $13, $14)",
                                    [ncmrwf_type, type, generated_time, site_id, rowTime, ghi, precipitation, surface_pressure, sealevel_pressure, humidity, temp_2m, low_cloud_cover_perc, wind_speed_10m, wind_speed_50m]);


                            })
                            .on('end', () => {
                                console.log(`${file}  processed`);
                            });
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
