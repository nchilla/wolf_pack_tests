const fs   = require('fs');
//sqlite for javascript library
const Database = require('better-sqlite3');
//file paths
const db_root='db-tests/';
const data_root='../wolf-pack/counts/';

const db = new Database(db_root+'all-pubs-and-counts.db');
// const db = new Database(db_root+'test.db');
// const db = new Database(':memory:');

let publications = Array.from(JSON.parse(fs.readFileSync('pub-list.json', 'utf8')).publications);

// creates a column string with a field for the publication key and a field for each month
let columns=['pub_key TEXT'];
for(let y=1975;y<2000;y++){
    for(let m=1;m<13;m++){
        // column name
        let col_name=`m${y}_${m<10?'0':''}${m} INTEGER`;
        columns.push(col_name)
    }
}
let columns_string=columns.join(',');

//for each n value, create a table, counts_n
    // for each month, create a column
for(let n=1; n<6;n++){
    let sqlname='counts_n'+n;
    let create_statement=`CREATE TABLE ${sqlname}(
        ${columns_string}
      )`;
    db.prepare(create_statement).run();
}

let columns_no_def=columns.map(a=>{
    return a.replace(' INTEGER','');
})
columns_no_def.splice(0,1);


// loop through each of the files in counts
// for each month
// console.log(columns)
for(let pub of publications){
    // get json
    insert_pub_counts(pub);
}

function insert_pub_counts(pub){
    let file_str=data_root+`${pub.key}.json`;
    if(fs.existsSync(file_str)){
        let counts_json=JSON.parse(fs.readFileSync(file_str, 'utf8'));
        let key =Object.keys(counts_json)[0];
        let counts_by_month=counts_json[key];
        let months=Object.keys(counts_by_month);
        
        let sql_cols=['pub_key'];
        let sql_vals={
            "n1":["'"+pub.key+"'"],
            "n2":["'"+pub.key+"'"],
            "n3":["'"+pub.key+"'"],
            "n4":["'"+pub.key+"'"],
            "n5":["'"+pub.key+"'"]
        }
        
        for(let month of months){
            let mstring=`m${month.replace('-','_')}`;
            
            if(columns_no_def.includes(mstring)){
                sql_cols.push(mstring);
                for(let n=1; n<6;n++){
                let n_count=counts_by_month[month][n+' gram'];
                n_count=typeof n_count=='number'?n_count:0;
                sql_vals["n"+n].push(n_count);
                }
            }
        }
       

        let sql_col_string=sql_cols.join(',');
        for(let n=1; n<6;n++){

            let table_name='counts_n'+n;
            let sql_val_string=sql_vals['n'+n].join(',');
   
            let insert_statement=`INSERT INTO ${table_name}(${sql_col_string}) VALUES (${sql_val_string})`;
            db.prepare(insert_statement).run();
         }
    }else{
        console.log('file does not exist?',pub.key)
    }
}



let summed_col_strings=columns_no_def.map(a=>{
    return `SUM(${a}) AS ${a}`
})


for(let n=1; n<6;n++){
    let statement=`INSERT INTO counts_n${n}
    SELECT 'all_publications' AS pub_key, ${summed_col_strings.join(',')} FROM counts_n${n}`;
    db.prepare(statement).run()
}