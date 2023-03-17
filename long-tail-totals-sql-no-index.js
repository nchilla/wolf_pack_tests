const fs   = require('fs');
//json streaming library
const {chain}  = require('stream-chain');
const {parser} = require('stream-json');
const {streamObject} = require('stream-json/streamers/StreamObject');
//sqlite for javascript library
const Database = require('better-sqlite3');

//file paths
const db_root='/Volumes/chilla/nico/db/';
const data_root='/Volumes/chilla/nico/cleaned/';

let name='pbs-newshour';

const db = new Database(db_root+'totals.db');

jsonProcessor(name)

async function jsonProcessor(file_name){
    let name_safe=name.replace(/-/g,'_');
    let months=[];

    // checks if tables have already been created
    let pub_init=db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='n1'`).get();
    
    //if not, creates a table for each n value
    if(!pub_init){
        for(n=1; n<6; n++){
            // create a new table for each n value from 1-5
            // with one column "gram" for the text strings
            db.prepare(`
            CREATE TABLE n${n}( 
                gram TEXT
            )
            `).run();
        }
    }

    //record start time for performance test
    let t0=performance.now();

    const pipeline =chain([
        fs.createReadStream(data_root+name+'.json'),
        parser(),
        streamObject(),
        //the following block runs for each month:
        data => {
            let date_safe=data.key.replace(/-/g,'_');
            months.push('m'+date_safe);
            console.log(date_safe,'========================')
            
            const insertGrams=db.transaction((statement,grams) => {
                for (const [gram, count] of Object.entries(grams)) statement.run({
                    gram:gram.replace(/'/g,"''"),
                    count:count
                })                
            });

            

            for(let n=1;n<6;n++){

                let grams=data.value[n];
                console.log('processing n='+n+'...');

                //add column for this month to corresponding n value table
                let col_exists=db.prepare(`SELECT * FROM n${n}`).get();
                if(col_exists==undefined||col_exists[`m${date_safe}`]===undefined){
                    db.prepare(`
                    ALTER TABLE n${n} 
                    ADD m${date_safe} INTEGER
                    `).run();
                }

                let statement=db.prepare(`
                INSERT INTO n${n} (gram, m${date_safe})
                VALUES (@gram,@count)
                ON CONFLICT(gram) DO UPDATE SET m${date_safe}=IFNULL(m${date_safe},0)+@count
            `);

                console.log('new transaction...');
                insertGrams(statement,grams);
                console.log('transaction complete');

            }

            
        }
    ]);

    pipeline.on('finish', () => {
        //logs performance
        let t1=performance.now();
        console.log(`finished dump in ${roundTo3((t1-t0)/1000)} seconds`);
        db.close();
      }
    );
}

function roundTo3(num){
    return Math.round(num*1000)/1000;
}