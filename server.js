const express = require('express');
const app = express();
const fs = require('fs');
const pool = require("./db");
const csv = require('csv-parser');
const moment = require('moment');
const bcrypt = require('bcrypt');

const generateBcryptHash = async (plainText) => {
    const saltRounds = 10; // Number of salt rounds, higher value increases security but also computation time
    try {
      const salt = await bcrypt.genSalt(saltRounds);
      const hash = await bcrypt.hash(plainText, salt);
      return hash;
    } catch (error) {
      console.error('Error generating bcrypt hash:', error);
      return null;
    }
  };

  main();
async function main() {
const key = await generateBcryptHash('wH8ZN9e2tIXo9J9Sc7Bb')
console.log(key);
}


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
                            console.log(`${file} of filepath - ${filePath} does not exist in utility_sites table`);
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

// CREATE TABLE forecast_temp (
//   id SERIAL PRIMARY KEY,
//   site_id INTEGER REFERENCES utility_sites(id),
//   block integer,
//   time timestamptz,
//   gen_final FLOAT,
//   ghi_final FLOAT,
//   poa_final FLOAT,
//   ghi_rev1 FLOAT,
//   ghi_rev0 FLOAT,
//   gen_rev0 FLOAT,
//   gen_rev1 FLOAT,
//   ghi_rev2 FLOAT,
//   gen_rev2 FLOAT,
//   ghi_rev3 FLOAT,
//   gen_rev3 FLOAT,
//   ghi_rev4 FLOAT,
//   gen_rev4 FLOAT,
//   ghi_rev5 FLOAT,
//   gen_rev5 FLOAT,
//   ghi_rev6 FLOAT,
//   gen_rev6 FLOAT,
//   ghi_rev7 FLOAT,
//   gen_rev7 FLOAT,
//   ghi_rev8 FLOAT,
//   gen_rev8 FLOAT,
//   ghi_rev9 FLOAT,
//   gen_rev9 FLOAT,
//   model_name VARCHAR(50),
//   created_at timestamptz DEFAULT NOW()
// );

//create a app.get route in nodejs add forecast data to forecast_temp table from /home/ec2-user/efs-solarad-output/csv/clients, loop through all clients folders inside you will find ml_forecasts folder (if there isn't skip that client) inside that you will find csv files for each site named as Solarad_Ichhawar_HeroFutureEnergies_Forecast_2023-09-28_ID.csv (Icchhawar is sitename, HeroFutureEnergies is company name, 2023-09-28 is date) get all of the row details from the csv and get the model_name from /home/ec2-user/efs_solaradoutput/records_ml/clients/HeroFutureEnergies/Icchhawar/Icchhawar_best_model_runs.csv (this file has a column named site_id and a column named model). To get a model for a date and a site_id get the model of the last occurence of the previous date, if it's not present, push null. If the date is not present, push null. If the site_id is not present, push null.
app.get('/addForecastData', async (req, res, next) => {
    try {
        const clients = {
            'CleanTech': ['Hatkarwadi'],
            'HeroFutureEnergies': ['Barod', 'Ichhawar'],
            'O2Power': ['Gorai'],
            'Sunsure': ['Banda'],
            'Avaada': ['Pavagada-1', 'Bhadla-2'],
            "BrookfieldRenewable": ["Jodhpur"],
            "HeroFutureEnergies": ["Ichhawar", "Barod", "Bhadla", "Siddipet"],
            'Refex': ['Bhilai'],
            'Sprng': ['Arinsun'],
            'Vibrant': ['Savner']
        }

        for (const [client, value] of Object.entries(clients)) {
            //loop through all the csv files in the ml_forecasts folder

            for (const site of value) {
                let dates = fs.readdir(`/home/ec2-user/efs-solarad-output/csv/clients/${client}/ml_forecasts/Solarad_${site}_${client}`);
                for (const date of dates) {
                let filepath = `/home/ec2-user/efs-solarad-output/csv/clients/${client}/ml_forecasts/Solarad_${site}_${client}_Forecast_${date}_ID.csv`;

                //get modelname from /home/ec2-user/efs_solaradoutput/records_ml/clients/${client}/${site}/${site}_best_model_runs.csv
                const siteIdQuery = await pool.query("SELECT id FROM utility_sites WHERE sitename = $1 AND company = $2", [site, client]);
                const siteId = siteIdQuery.rows[0].id;
                let modelname = null;
                const modelFile = `/home/ec2-user/efs_solaradoutput/records_ml/clients/${client}/${site}/${site}_best_model_runs.csv`;
                    

                //get previous date
                const dateParts = date.split('-');
                const year = dateParts[0];
                const month = dateParts[1];
                const day = dateParts[2];
                const prevDate = moment(`${year}-${month}-${day}`).subtract(1, 'days').format('YYYY-MM-DD');

                const readableStreamForModel = fs.createReadStream(modelFile)

                if(fs.existsSync(modelFile)) {
                    readableStreamForModel
                    .pipe(csv())
                    .on('data', async (row) => {
                        if(row['site_id'] === siteId && row['train_date'] === prevDate) {
                            modelname = row['model'];
                        }
                    })
                    .on('end', () => {
                        console.log(`CSV file successfully processed for ${site} and ${client}`);
                    });
                }


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
                        const time = row['time'];
                        const block = row['block'];
                        const gen_final = parseFloat(row['Gen Final']);
                        const ghi_final = parseFloat(row['GHI Final']);
                        const poa_final = parseFloat(row['POA Final']);
                        const ghi_rev1 = parseFloat(row['Gen Rev0']);
                        const ghi_rev0 = parseFloat(row['GHI Rev0']);
                        const gen_rev0 = parseFloat(row['Gen Rev1']);
                        const gen_rev1 = parseFloat(row['GHI Rev1']);
                        const ghi_rev2 = parseFloat(row['Gen Rev2']);
                        const gen_rev2 = parseFloat(row['GHI Rev2']);
                        const ghi_rev3 = parseFloat(row['Gen Rev3']);
                        const gen_rev3 = parseFloat(row['GHI Rev3']);
                        const ghi_rev4 = parseFloat(row['Gen Rev4']);
                        const gen_rev4 = parseFloat(row['GHI Rev4']);
                        const ghi_rev5 = parseFloat(row['Gen Rev5']);
                        const gen_rev5 = parseFloat(row['GHI Rev5']);
                        const ghi_rev6 = parseFloat(row['Gen Rev6']);
                        const gen_rev6 = parseFloat(row['GHI Rev6']);
                        const ghi_rev7 = parseFloat(row['Gen Rev7']);
                        const gen_rev7 = parseFloat(row['GHI Rev7']);
                        const ghi_rev8 = parseFloat(row['Gen Rev8']);
                        const gen_rev8 = parseFloat(row['GHI Rev8']);
                        const ghi_rev9 = parseFloat(row['Gen Rev9']);
                        const gen_rev9 = parseFloat(row['GHI Rev9']);
                        
                        //
                        await pool.query("INSERT INTO forecast_temp (site_id, block, time, gen_final, ghi_final, poa_final, ghi_rev1, ghi_rev0, gen_rev0, gen_rev1, ghi_rev2, gen_rev2, ghi_rev3, gen_rev3, ghi_rev4, gen_rev4, ghi_rev5, gen_rev5, ghi_rev6, gen_rev6, ghi_rev7, gen_rev7, ghi_rev8, gen_rev8, ghi_rev9, gen_rev9, model_name) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9 , $10, $11, $12, $13, $14, $15, $16, $17 , $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)", [siteId, block, time, gen_final, ghi_final, poa_final, ghi_rev1, ghi_rev0, gen_rev0, gen_rev1, ghi_rev2, gen_rev2, ghi_rev3, gen_rev3, ghi_rev4, gen_rev4, ghi_rev5, gen_rev5, ghi_rev6, gen_rev6, ghi_rev7, gen_rev7, ghi_rev8, gen_rev8, ghi_rev9, gen_rev9, modelname]);
                    }
                    )
                    .on('end', () => {
                        console.log(`CSV file successfully processed for ${site} and ${client}`);
                    });
                }
            }
        }

        res.send("Done");

    } catch (err) {
        console.log(err.message);
        return;
    }
}
);



const port = process.env.PORT || 3001;
const host = process.env.HOST || `localhost`;


app.listen(5001, () => {
    console.log(`Server is up on port ${5001}`);
});
