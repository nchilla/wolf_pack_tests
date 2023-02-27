const fs   = require('fs');
const {chain}  = require('stream-chain');
const {parser} = require('stream-json');
const {streamObject} = require('stream-json/streamers/StreamObject');
const Database = require('better-sqlite3');



const db_root='/Volumes/chilla/nico/db/';
const data_root='/Volumes/chilla/nico/cleaned/';


const db = new Database(db_root+'chicago-no-repeats.db');




importPub('chicago-defender');

// db.close();

//for each publication,
async function importPub(name){
    let name_safe=name.replace(/-/g,'_');
    // for each n value
    let pub_init=db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='${name_safe}_n1'`).get();
    
    if(!pub_init){
        for(n=1; n<6; n++){
            let declaration=`
            CREATE TABLE ${name_safe}_n${n}( 
                gram TEXT
            )
            `;
            db.prepare(declaration).run();
            //create a new table with a term column
        }
    }

    //stream through each of the dates
    

    let t0=performance.now();
    const pipeline =chain([
        fs.createReadStream(data_root+name+'.json'),
        parser(),
        streamObject(),
        data => {
            // for each date
            let date_safe=data.key.replace(/-/g,'_');
            console.log(date_safe,'========================')

            //for each n value
            for(let n=1;n<6;n++){
                //add column for that date to corresponding table
                let col_exists=db.prepare(`SELECT * FROM ${name_safe}_n${n}`).get();
                // console.log(col_exists)
                if(col_exists==undefined||col_exists[`m${date_safe}`]===undefined){
                    db.prepare(`
                    ALTER TABLE ${name_safe}_n${n} 
                    ADD m${date_safe} INTEGER
                    `).run();
                }

                
                let grams=data.value[n];
                console.log('processing n='+n+'...');
                //for each gram
                    //1. search all ngram table to see if it exists
                        //if not, add it
                        //either way, get its ID

                    for(const [gram, count] of Object.entries(grams)){
                        let gram_safe=gram.replace(/'/g,"''");
                        let val=db.prepare(
                            `SELECT gram 
                             FROM ${name_safe}_n${n} 
                             WHERE gram='${gram_safe}'
                        `).get()?.gram;
                        
                        if(val==undefined){
                            db.prepare(`
                                INSERT INTO ${name_safe}_n${n} (gram, m${date_safe})
                                VALUES ('${gram_safe}',${count})
                            `).run();
                        }else{
                            db.prepare(`UPDATE ${name_safe}_n${n} 
                            SET m${date_safe} = ${count} 
                            WHERE gram='${gram_safe}'`).run();
                        }
                        

                        
                        
                    }
                    
                    //2. search pub-nval table for ID
                        //if it doesn't exist, add it
                    //3. enter count for that ID / month

                
                
               

            }
        }
    ]);

    pipeline.on('finish', () => {
        let t1=performance.now();
        console.log(`finished in ${t1-t0}ms`);
        db.close();
      }
    );
    
    
        
        //for each n value
            //add column for that date to corresponding table
            //for each term
                //1. search all ngram table to see if it exists
                    //if not, add it
                    //either way, get its ID
                //2. search pub-nval table for ID
                    //if it doesn't exist, add it
                //3. enter count for that ID / month


                
    



        //a table of ngrams (rows) x available dates (columns)
       //should use consistent naming scheme using dash-name-of-pub from file name + # for the n value

    //1. use stream to loop through each of the dates
    //
}


// function processDate(){

// }



