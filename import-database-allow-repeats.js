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

//creates or locates database file
let name='chicago-defender'
const db = new Database(db_root+name+'.db');
// db.pragma('synchronous=OFF');

//starts processing chicago defender
importPub(name);

//processes one publication, using its name slug
async function importPub(name){
    //changes hyphens to underscores for sql naming convention
    let name_safe=name.replace(/-/g,'_');

    // checks if tables have already been created
    let pub_init=db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='${name_safe}_n1'`).get();
    
    //if not, creates a table for each n value
    if(!pub_init){
        for(n=1; n<6; n++){
            // create a new table for each n value from 1-5
            // with one column "gram" for the text strings
            db.prepare(`
            CREATE TABLE ${name_safe}_n${n}( 
                gram TEXT
            )
            `).run();
        }
    }

    //record start time for performance test
    let t0=performance.now();

    //using streaming library to read the JSON in chunks
    const pipeline =chain([
        fs.createReadStream(data_root+name+'.json'),
        parser(),
        streamObject(),
        //the following block runs for each month:
        data => {
            let date_safe=data.key.replace(/-/g,'_');
            console.log(date_safe,'========================')
            
            //for each n value from 1 to 5
            const insertGrams=db.transaction((statement,grams) => {
                for (const [gram, count] of Object.entries(grams)) statement.run({
                    gram:gram.replace(/'/g,"''"),
                    count:count
                })                
            });


            for(let n=1;n<6;n++){
                //add column for this month to corresponding n value table
                let col_exists=db.prepare(`SELECT * FROM ${name_safe}_n${n}`).get();
                if(col_exists==undefined||col_exists[`m${date_safe}`]===undefined){
                    db.prepare(`
                    ALTER TABLE ${name_safe}_n${n} 
                    ADD m${date_safe} INTEGER
                    `).run();
                }

                //the ngram data for this this month + n value
                let grams=data.value[n];
                console.log('processing n='+n+'...');

                //defines an SQL insert statement to add a new row corresponding to a single ngram in a single month
                //(it's faster to insert them this way and then consolidate the rows in SQL)
                let statement=db.prepare(`
                    INSERT INTO ${name_safe}_n${n} (gram, m${date_safe})
                    VALUES (@gram,@count)
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
        console.log(`finished in ${t1-t0}ms`);
        db.close();
      }
    );
    
}




