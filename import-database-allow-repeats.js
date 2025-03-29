const fs   = require('fs');
//json streaming library
const {chain}  = require('stream-chain');
const {parser} = require('stream-json');
const {pick}   = require('stream-json/filters/Pick');
const {streamObject} = require('stream-json/streamers/StreamObject');
//sqlite for javascript library
const Database = require('better-sqlite3');

//file paths
const db_root='db-tests/';
const data_root='../wolf-pack/long-tails/';

//creates or locates database file

const db = new Database(db_root+'all-pubs.db');
// db.pragma('synchronous=OFF');

//starts processing chicago defender
// importPub(name);


let i=60;
let publications = Array.from(JSON.parse(fs.readFileSync('pub-list.json', 'utf8')).publications);

let name=publications[60].key;
importPub(name);

// async function all_pubs_import(pubs){
//     for(let pub of pubs){
//         name=pub.key;
//         await importPub(name);
//     }
// }

//processes one publication, using its name slug
async function importPub(name){
    //changes hyphens to underscores for sql naming convention
    // let name=name.replace(/-/g,'_');
    let months=[];

    // checks if tables have already been created
    let pub_init=db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='${name}_n1_dump'`).get();
    
    //if not, creates a table for each n value
    if(!pub_init){
        for(n=1; n<6; n++){
            // create a new table for each n value from 1-5
            // with one column "gram" for the text strings
            db.prepare(`
            CREATE TABLE ${name}_n${n}_dump( 
                gram TEXT
            )
            `).run();
        }
    }

    //record start time for performance test
    let t0=performance.now();

    console.log(name,'==========================')
    console.log('dumping data...')
    //using streaming library to read the JSON in chunks
    const pipeline =chain([
        fs.createReadStream(data_root+name+'.json'),
        parser(),
        pick({filter: 'months'}),
        streamObject(),
        //the following block runs for each month:
        data => {
            let date_safe=data.key.replace(/-/g,'_');
            months.push('m'+date_safe);
            console.log(date_safe)
            
            //for each n value from 1 to 5
            const insertGrams=db.transaction((statement,grams) => {
                // console.log(statement,grams);
                for (const [gram, count] of Object.entries(grams)) statement.run({
                    gram:gram.replace(/'/g,"''"),
                    count:count
                })
            });


            for(let n=1;n<6;n++){
                //add column for this month to corresponding n value table
                let col_exists=db.prepare(`SELECT * FROM ${name}_n${n}_dump`).get();
                if(col_exists==undefined||col_exists[`m${date_safe}`]===undefined){
                    db.prepare(`
                    ALTER TABLE ${name}_n${n}_dump 
                    ADD m${date_safe} INTEGER
                    `).run();
                }

                //the ngram data for this this month + n value
               
                let grams=data.value[n+' gram'];
                // console.log('processing n='+n+'...');

                //defines an SQL insert statement to add a new row corresponding to a single ngram in a single month
                //(it's faster to insert them this way and then consolidate the rows in SQL)
                let statement=db.prepare(`
                    INSERT INTO ${name}_n${n}_dump (gram, m${date_safe})
                    VALUES (@gram,@count)
                `);

                // console.log('new transaction...');
                insertGrams(statement,grams);
                // console.log('transaction complete');
            }

            
        }
    ]);

    pipeline.on('finish', () => {
        //logs performance
        let t1=performance.now();
        console.log(`finished dump in ${roundTo3((t1-t0)/1000)} seconds`);
        console.log('beginning consolidation and indexing');
        consolidate_and_index();
        let t2=performance.now();
        console.log(`finished consolidating in ${roundTo3((t2-t1)/1000)} seconds`);
        // console.log('beginning vacuum');
        // db.prepare(`VACUUM`).run();
        let t3=performance.now();
        // console.log(`finished vacuuming in ${ roundTo3((t3-t2)/1000) } seconds`);
        
        i++;
        if(i<publications.length){
            name=publications[i].key;
            importPub(name);
        }
        
        
        // db.close();
      }
    );


    function consolidate_and_index(){
        let cols='gram, '+months.map(a=>'MAX('+a+') AS '+a).join(', ');
        for(let n=1;n<6;n++){
            console.log(`consolidating ${name}_n${n}...`);
            db.prepare(`CREATE TABLE ${name}_n${n} AS
            SELECT ${cols}
            FROM ${name}_n${n}_dump
            GROUP BY gram;`).run();
            console.log(`creating index for ${name}_n${n}...`);
            db.prepare(`CREATE INDEX index_${name}_n${n} ON ${name}_n${n} (gram);`).run();
            db.prepare(`DROP TABLE ${name}_n${n}_dump`).run();
        }
    }
    
}




function roundTo3(num){
    return Math.round(num*1000)/1000;
}