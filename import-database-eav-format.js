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
const totals_root='../wolf-pack/counts/';

let verbose_error_logs=false


// TESTING FILES
// let pub_list_json='pub-list-ltd.json';
// const db = new Database(':memory:');
// const db = new Database(db_root+'test-eav.db');


// FINAL FILES
let pub_list_json='pub-list.json';
const db = new Database(db_root+'all-pubs-eav.db');


let publications = Array.from(JSON.parse(fs.readFileSync(pub_list_json, 'utf8')).publications);


initial_db_setup();

let pub_key;


all_pubs_import(publications);


async function all_pubs_import(pubs){

    let t1=performance.now();
    console.log('--- beginning gram data import...')
    

    for(let pub of pubs){
        pub_key=pub.key;
        await import_pub_eav(pub_key);
    }

    console.log('--- finished with gram data import. importing total publication counts...')

    create_total_counts_table();

    console.log('--- finished importing total counts. now calculating aggregate counts...')

    create_aggregate_count('all_publications',publications);
    create_aggregate_count('broadcast',publications.filter(a=>a.types.includes('broadcast')));
    create_aggregate_count('print',publications.filter(a=>a.types.includes('print')));

    console.log('--- finished calculating aggregates. now vacuuming...')

    db.prepare('VACUUM').run();

    let t2=performance.now();
    console.log(`--- all done. completed in ${round_to_3((t2-t1)/60000)} minutes`);

}





function initial_db_setup(){
    for(n=1; n<6; n++){
        // create a new table for each n value from 1-5
        // with one column "gram" for the text strings
        db.prepare(`
        CREATE TABLE _grams_n${n}( 
            id INTEGER PRIMARY KEY,
            gram TEXT UNIQUE
        )`).run();

        

    }

    db.prepare(`
    CREATE TABLE _pubs( 
        id INTEGER PRIMARY KEY,
        pub_key TEXT UNIQUE
    )`).run();
}







function import_pub_eav(pub_key){

    return  new Promise((resolve) => {
        //creates a table for each n value
        for(n=1; n<6; n++){
            // create a new table for each n value from 1-5
            // with one column "gram" for the text strings
            db.prepare(`
            CREATE TABLE ${pub_key}_n${n}( 
                gram_id INTEGER,
                month TEXT,
                count INTEGER,
                    FOREIGN KEY (gram_id)
                    REFERENCES _grams_n${n} (id)
            )
            `).run();
        }

        //record start time for performance monitoring
        let t0=performance.now();
        console.log('processing '+pub_key+'...')

        const pipeline =chain([
            fs.createReadStream(data_root+pub_key+'.json'),
            parser(),
            pick({filter: 'months'}),
            streamObject(),
            //the following block runs for each month:
            data => {
                let month=data.key;

                //for each n value from 1 to 5
                for(let n=1;n<6;n++){
                    let grams=data.value[n+' gram'];

                    let find_in_root=db.prepare(`SELECT id FROM _grams_n${n} WHERE gram = @gram`);

                    let insert_into_root=db.prepare(`INSERT INTO _grams_n${n} (gram)
                    VALUES (@gram) RETURNING id`);

                    let insert_into_pub=db.prepare(`INSERT INTO ${pub_key}_n${n} (gram_id,month,count)
                    VALUES (@gram_id,@month,@count)`);

                    const insert_grams=db.transaction((grams) => {
                    
                        
                        for (const [gram, count] of Object.entries(grams)){
                            let gram_id=find_in_root.get({gram:gram.replace(/'/g,"''")})?.id;
                            if(gram_id==undefined) gram_id=insert_into_root.get({gram:gram.replace(/'/g,"''")}).id;
                            insert_into_pub.run({
                                gram_id,
                                month,
                                count
                            });
                        }
                        
                    });

                    insert_grams(grams);

                    
                    
                
                }


                

                
            }
        ]);

        pipeline.on('finish', () => {
            //logs performance
            let t1=performance.now();
            console.log(`finished dump in ${round_to_3((t1-t0)/1000)} seconds`);
            console.log('beginning indexing');
            create_indexes();
            let t2=performance.now();
            console.log(`finished indexing in ${round_to_3((t2-t1)/1000)} seconds`);        

            resolve(true);
        }
        );

        function create_indexes(){
            for(let n=1;n<6;n++){
                console.log(`creating index for ${pub_key}_n${n}...`);
                db.prepare(`CREATE INDEX ${pub_key}_n${n}_index ON ${pub_key}_n${n} (gram_id, month);`).run();
            }
        }
    });
    
    


}


function create_aggregate_count(name,pubs){
    if(pubs.length>1){
        const transaction=db.transaction(() => {
            let aggregate_id=db.prepare(`INSERT INTO _pubs (pub_key)
            VALUES ('_sum_${name}') RETURNING id`).get().id;


            for(let n=1;n<6;n++){
                let unions=pubs.map(a=>{
                    return `SELECT * FROM ${a.key}_n${n}`;
                })
    
                db.prepare(`CREATE TABLE _sum_${name}_n${n} (
                    gram_id INTEGER,
                        month TEXT,
                        count INTEGER,
                            FOREIGN KEY (gram_id)
                            REFERENCES _grams_n${n} (id)
                )`).run();
    
        
                db.prepare(`
                    INSERT INTO _sum_${name}_n${n}
                    SELECT gram_id, month, SUM(count) AS count
                    FROM (
                    ${unions.join('\nUNION ALL \n')}
                    ) GROUP BY gram_id, month;
                `).run();
    
                db.prepare(`CREATE INDEX _sum_${name}_n${n}_index ON _sum_${name}_n${n} (gram_id, month);`).run();
                
                let where_string=pubs.map(a=>`_totals_n${n}.pub_id = '${a.id}'`).join(' OR ');

                // create and add sums of totals

                db.prepare(`
                    INSERT INTO _totals_n${n}
                    SELECT ${aggregate_id} AS pub_id, month, SUM(count) AS count
                    FROM _totals_n${n} 
                    WHERE ${where_string}
                    GROUP BY month;
                `).run();
            
            }


            
        
        
        
        
        });
    
        transaction();
    }

    

    
}


function create_total_counts_table(){
    for(let n=1; n<6;n++){

        let create_statement=`CREATE TABLE _totals_n${n}(
            pub_id INTEGER,
            month TEXT,
            count INTEGER,
                FOREIGN KEY (pub_id)
                REFERENCES _pubs (id)
          )`;
        db.prepare(create_statement).run();
    }

    for(let pub of publications){
        console.log(`adding ${pub.key} totals...`)
        insert_pub_counts(pub);
    }

    function insert_pub_counts(pub){
        let pub_id=db.prepare(`INSERT INTO _pubs (pub_key)
        VALUES ('${pub.key}') RETURNING id`).get().id;
        pub.id=pub_id;


        let file_str=totals_root+`${pub.key}.json`;
        let totals_json=JSON.parse(fs.readFileSync(file_str, 'utf8'));
        let outer_key =Object.keys(totals_json)[0];
        let totals_by_month=totals_json[outer_key];
        let months=Object.keys(totals_by_month);

        let values={
            "1":[],
            "2":[],
            "3":[],
            "4":[],
            "5":[]
        }

        for(let month of months){
            for(let n=1; n<6;n++){
                let n_count=totals_by_month[month][n+' gram'];
                if(typeof n_count=='number') values[n].push(`(${pub_id}, '${month}', ${n_count})`)
                else if(verbose_error_logs) console.log(`n count is not a number:' ${month}, n=${n}`)
            }
        }

        
        for(let n=1; n<6;n++){
            if(values[n].length>0) db.prepare(`INSERT INTO _totals_n${n}(pub_id,month,count) VALUES ${values[n].join(', ')}`).run();
         }

    }

    // index totals tables
    for(let n=1; n<6;n++){
        db.prepare(`CREATE INDEX _totals_n${n}_index ON _totals_n${n} (pub_id, month);`).run();
    }    
}


function round_to_3(num){
    return Math.round(num*1000)/1000;
}